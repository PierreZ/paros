//! Tests of the `Host`: assignments, recovery from records, routing by
//! journal, the flight rule on the effects, calls and the timer.

use paros_journal::register::Ballot;
use paros_journal::roles::{LeaderPhase, Message, PersistId};
use paros_journal::{Grid, OpeningCall, RELEASE, Slot};

use super::*;
use crate::layout::{self, Kind, acceptor_ops};

const J: JournalId = JournalId(1);
const K: JournalId = JournalId(2);
const CONFIG: HostConfig = HostConfig {
    tick: 10,
    resend_page: 4,
    window: 64,
};

fn at(journal: JournalId, node: u64) -> Address {
    Address::new(journal, NodeId(node))
}

/// A register value with segment 1: acceptors 1 to 3, replicas 11 to 13.
fn value() -> RegisterValue {
    let value = RegisterValue::new()
        .apply(&Change::Elect { holder: NodeId(0) })
        .expect("an election");
    value
        .apply(&Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: None,
                grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a grid"),
                replicas: vec![NodeId(11), NodeId(12), NodeId(13)],
            },
            release: RELEASE,
        })
        .expect("a reshape")
}

fn acceptor(address: Address) -> Input {
    Input::Assign(Assignment::Acceptor { address, page: 16 })
}

fn recovered(address: Address) -> Input {
    Input::Recovered {
        address,
        entries: Vec::new(),
    }
}

fn prepare(journal: JournalId, to: u64) -> Input {
    Input::Message(Envelope {
        journal,
        from: NodeId(100),
        to: NodeId(to),
        message: Message::Prepare {
            ballot: Ballot {
                round: 1,
                node: NodeId(100),
            },
            from: Slot(0),
            segment: SegmentId(1),
        },
    })
}

#[test]
fn an_acceptor_starts_with_a_read_and_routes_by_journal() {
    let mut host = Host::new(CONFIG);
    let effects = host.step(Time(5), acceptor(at(J, 1)));
    assert_eq!(effects, vec![Effect::Read { address: at(J, 1) }]);
    assert!(host.serves(at(J, 1)));
    assert!(host.acceptor(at(J, 1)).is_none(), "it still recovers");
    assert!(
        host.step(Time(6), prepare(J, 1)).is_empty(),
        "a role that recovers loses its messages"
    );
    assert_eq!(
        host.step(Time(6), recovered(at(J, 1))),
        Vec::new(),
        "an acceptor sets no timer"
    );
    assert!(host.acceptor(at(J, 1)).is_some());
    assert_eq!(
        host.step(Time(6), recovered(at(J, 1))),
        Vec::new(),
        "a second read result changes nothing"
    );

    // The same node in another journal is another role.
    host.step(Time(7), acceptor(at(K, 1)));
    host.step(Time(7), recovered(at(K, 1)));
    assert_eq!(host.journals().collect::<Vec<_>>(), vec![J, K]);

    let effects = host.step(Time(8), prepare(K, 1));
    let [
        Effect::Persist { address, id, ops },
        Effect::Send {
            sender: _,
            envelope,
            after,
        },
    ] = effects.as_slice()
    else {
        panic!("a promise persists, then waits on its batch: {effects:?}");
    };
    assert_eq!(*address, at(K, 1));
    assert_eq!(*after, Some(*id));
    assert_eq!(envelope.journal, K);
    assert_eq!(envelope.to, NodeId(100));
    assert!(matches!(envelope.message, Message::Promise { .. }));
    assert_eq!(ops.last().map(|op| op.key.kind), Some(Kind::Durable));
    assert_eq!(
        host.acceptor(at(J, 1)).map(JournalAcceptor::promised),
        Some(Ballot::zero()),
        "the acceptor of the other journal is untouched"
    );
}

#[test]
fn an_acceptor_recovers_its_promise_from_its_records() {
    let address = at(J, 2);
    let promised = Ballot {
        round: 4,
        node: NodeId(100),
    };
    let ops = acceptor_ops(
        address,
        PersistId(6),
        &[paros_journal::roles::AcceptorOp::Write(
            paros_journal::roles::AcceptorWrite::SetPromise(promised),
        )],
    );
    let entries = ops
        .into_iter()
        .map(|op| (op.key, Entry::Value(op.value)))
        .collect::<Vec<_>>();
    let mut host = Host::new(CONFIG);
    host.step(Time(0), acceptor(address));
    host.step(Time(0), Input::Recovered { address, entries });
    assert_eq!(
        host.acceptor(address).map(JournalAcceptor::promised),
        Some(promised)
    );
    // A Prepare at the promised ballot writes nothing and waits on the
    // durable batch.
    let effects = host.step(
        Time(1),
        Input::Message(Envelope {
            journal: J,
            from: NodeId(100),
            to: NodeId(2),
            message: Message::Prepare {
                ballot: promised,
                from: Slot(0),
                segment: SegmentId(1),
            },
        }),
    );
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Send {
                after: Some(PersistId(6)),
                ..
            }]
        ),
        "{effects:?}"
    );
}

#[test]
fn a_damaged_record_stops_the_role() {
    let address = at(J, 3);
    let mut host = Host::new(CONFIG);
    host.step(Time(0), acceptor(address));
    let key = Key::new(address, Kind::Vote, 4);
    let effects = host.step(
        Time(0),
        Input::Recovered {
            address,
            entries: vec![(key, Entry::Corrupt)],
        },
    );
    assert_eq!(
        effects,
        vec![Effect::Stopped {
            address,
            why: Stop::Damaged(Damage { keys: vec![key] }),
        }]
    );
    assert!(!host.serves(address));
    host.step(Time(0), acceptor(address));
    let effects = host.step(Time(0), Input::Unreadable { address });
    assert_eq!(
        effects,
        vec![Effect::Stopped {
            address,
            why: Stop::Unreadable,
        }]
    );
}

#[test]
fn a_leader_needs_a_register_value_and_answers_calls() {
    let leader = at(J, 100);
    let campaign = Campaign {
        ballot: Ballot {
            round: 1,
            node: NodeId(100),
        },
        segment: SegmentId(1),
        from: Slot(0),
    };
    let assign = Input::Assign(Assignment::Leader {
        address: leader,
        proxies: Vec::new(),
        campaign: campaign.clone(),
    });
    let mut host = Host::new(CONFIG);
    let effects = host.step(Time(0), assign.clone());
    assert_eq!(
        effects.last(),
        Some(&Effect::Stopped {
            address: leader,
            why: Stop::NoCampaign,
        }),
        "no register value yet"
    );
    host.step(
        Time(0),
        Input::Observe {
            journal: J,
            value: value(),
        },
    );
    assert_eq!(host.value(J), Some(&value()));
    let effects = host.step(Time(0), assign);
    let prepares = effects
        .iter()
        .filter(|e| matches!(e, Effect::Send { envelope, after: None, .. } if matches!(envelope.message, Message::Prepare { .. })))
        .count();
    assert_eq!(prepares, 3, "one Prepare to each acceptor of the grid");
    assert_eq!(
        host.leader(leader).map(|l| l.phase().clone()),
        Some(LeaderPhase::Phase1)
    );

    // In Phase 1 the leader takes no call; nor does a role that is no leader.
    let call = |address, call| Input::Call {
        address,
        call,
        command: Command::Noop,
    };
    for (address, id) in [(leader, 1), (at(J, 1), 2)] {
        assert_eq!(
            host.step(Time(1), call(address, id)),
            vec![Effect::Output {
                address,
                output: Output::Reply {
                    call: id,
                    outcome: CallOutcome::NotLeader,
                },
                after: None,
            }]
        );
    }

    assert_eq!(
        host.step(Time(2), Input::Unassign(leader)),
        vec![Effect::Stopped {
            address: leader,
            why: Stop::Unassigned,
        }]
    );
    assert_eq!(host.step(Time(2), Input::Unassign(leader)), Vec::new());
}

#[test]
fn the_timer_of_a_replica_fires_it_and_it_sets_it_again() {
    let mut host = Host::new(CONFIG);
    host.step(
        Time(0),
        Input::Observe {
            journal: J,
            value: value(),
        },
    );
    let replica = at(J, 11);
    let effects = host.step(
        Time(0),
        Input::Assign(Assignment::Replica {
            address: replica,
            segment: SegmentId(1),
            set: vec![NodeId(11), NodeId(12), NodeId(13)],
            initial: Leader::Unset,
        }),
    );
    let segment = Address::replica(J, NodeId(11), SegmentId(1));
    assert_eq!(effects, vec![Effect::Read { address: segment }]);
    let effects = host.step(Time(0), recovered(segment));
    assert_eq!(
        effects.last(),
        Some(&Effect::SetTimer {
            address: segment,
            at: Time(10),
        }),
        "the replica sets its timer when it starts"
    );
    let base = host.replica(replica).and_then(SegmentReplica::front);
    assert_eq!(
        base,
        Some(Slot(0)),
        "the replica took its base from the value"
    );
    let effects = host.step(Time(10), Input::TimerFired { address: segment });
    let pulls = effects
        .iter()
        .filter(|e| matches!(e, Effect::Send { envelope, .. } if matches!(envelope.message, Message::Pull { .. })))
        .count();
    assert_eq!(pulls, 2, "the replica asks its two peers");
    assert_eq!(
        effects.last(),
        Some(&Effect::SetTimer {
            address: segment,
            at: Time(20),
        })
    );
}

#[test]
fn a_failed_commit_stops_the_role() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), acceptor(at(J, 1)));
    host.step(Time(0), recovered(at(J, 1)));
    assert_eq!(
        host.step(Time(1), Input::Failed { address: at(J, 1) }),
        vec![Effect::Stopped {
            address: at(J, 1),
            why: Stop::Failed,
        }]
    );
    assert_eq!(host.step(Time(1), prepare(J, 1)), Vec::new());
}

#[test]
fn a_value_reaches_only_the_roles_of_its_journal() {
    let mut host = Host::new(CONFIG);
    let replica = |journal| {
        Input::Assign(Assignment::Replica {
            address: at(journal, 11),
            segment: SegmentId(1),
            set: vec![NodeId(11), NodeId(12), NodeId(13)],
            initial: Leader::Unset,
        })
    };
    for journal in [J, K] {
        host.step(Time(0), replica(journal));
        host.step(
            Time(0),
            recovered(Address::replica(journal, NodeId(11), SegmentId(1))),
        );
    }
    assert_eq!(
        host.addresses().collect::<Vec<_>>(),
        vec![
            Address::replica(J, NodeId(11), SegmentId(1)),
            Address::replica(K, NodeId(11), SegmentId(1))
        ]
    );
    host.step(
        Time(0),
        Input::Observe {
            journal: K,
            value: value(),
        },
    );
    let front = |journal| {
        host.replica(at(journal, 11))
            .and_then(SegmentReplica::front)
    };
    assert_eq!(front(K), Some(Slot(0)), "the replica of K took its base");
    assert_eq!(front(J), None, "the replica of J did not");
    assert_eq!(host.value(J), None);
}

#[test]
fn a_replica_node_holds_one_replica_for_each_segment() {
    let mut host = Host::new(CONFIG);
    let set = vec![NodeId(11), NodeId(12), NodeId(13)];
    for segment in [SegmentId(1), SegmentId(2)] {
        host.step(
            Time(0),
            Input::Assign(Assignment::Replica {
                address: at(J, 11),
                segment,
                set: set.clone(),
                initial: Leader::Unset,
            }),
        );
        host.step(Time(0), recovered(Address::replica(J, NodeId(11), segment)));
    }
    let first = Address::replica(J, NodeId(11), SegmentId(1));
    let second = Address::replica(J, NodeId(11), SegmentId(2));
    assert_eq!(host.addresses().collect::<Vec<_>>(), vec![first, second]);
    assert_eq!(
        host.replica(at(J, 11)).map(SegmentReplica::segment),
        Some(SegmentId(2)),
        "a lookup with no segment finds the newest replica of the node"
    );
    assert_eq!(
        host.replica(first).map(SegmentReplica::segment),
        Some(SegmentId(1))
    );
    let pull = |segment| Envelope {
        journal: J,
        from: NodeId(12),
        to: NodeId(11),
        message: Message::Pull {
            segment,
            from: Slot(0),
        },
    };
    assert_eq!(
        pull(SegmentId(1)).receiver(),
        first,
        "a Pull names its replica"
    );
    assert_eq!(pull(SegmentId(2)).receiver(), second);
    let stopped: Vec<Address> = host
        .step(Time(1), Input::Unassign(at(J, 11)))
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Stopped { address, .. } => Some(address),
            _ => None,
        })
        .collect();
    assert_eq!(stopped, vec![first, second], "the node stops every replica");
    assert_eq!(host.addresses().count(), 0);
}

/// `value()` with segment 2 opened, and segment 1 sealed at 2, its
/// boundary written and retired.
fn retired() -> RegisterValue {
    let value = value();
    value
        .apply(&Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a grid"),
                replicas: vec![NodeId(21), NodeId(22), NodeId(23)],
            },
            release: RELEASE,
        })
        .and_then(|v| {
            v.apply(&Change::Seal {
                segment: SegmentId(1),
                end: Slot(2),
            })
        })
        .and_then(|v| {
            v.apply(&Change::Boundary {
                segment: SegmentId(1),
                state: paros_journal::BoundaryState::fresh(Leader::Unset),
            })
        })
        .and_then(|v| {
            v.apply(&Change::Retire {
                segment: SegmentId(1),
                holders: 2,
                unrepaired_damage: false,
            })
        })
        .expect("segment 1 retires")
}

/// `value` with replica 13 of segment 1 replaced by 20.
fn healed(value: &RegisterValue) -> RegisterValue {
    value
        .apply(&Change::Rereplicate {
            segment: SegmentId(1),
            read: value.segment(SegmentId(1)).expect("segment 1").clone(),
            term: value.term().number,
            remove: NodeId(13),
            add: NodeId(20),
        })
        .expect("a heal")
}

const NEW: Address = Address {
    journal: J,
    node: NodeId(20),
    segment: Some(SegmentId(1)),
};

fn copy(number: u64) -> CopyId {
    CopyId { term: 1, number }
}

fn join(number: u64) -> Input {
    Input::Join {
        address: NEW,
        set: vec![NodeId(11), NodeId(12), NodeId(13)],
        initial: Leader::Unset,
        copy: copy(number),
    }
}

fn observe(value: RegisterValue) -> Input {
    Input::Observe { journal: J, value }
}

/// A `Host` that holds `value` and runs copy 5 on node 20.
fn joined(value: RegisterValue) -> Host {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(value));
    let effects = host.step(Time(0), join(5));
    assert!(effects.contains(&Effect::Read { address: NEW }));
    let effects = host.step(Time(0), recovered(NEW));
    let [Effect::Persist { ops, .. }, ..] = effects.as_slice() else {
        panic!("the copy is on the disk first: {effects:?}");
    };
    assert_eq!(ops[0].key.kind, Kind::Joining);
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(5))
    );
    host
}

#[test]
fn a_join_starts_only_on_a_retired_segment_that_does_not_name_the_node() {
    let mut host = Host::new(CONFIG);
    assert!(host.step(Time(0), join(5)).is_empty(), "no value: nothing");
    host.step(Time(0), observe(value()));
    assert!(host.step(Time(0), join(5)).is_empty(), "in force: nothing");
    host.step(Time(0), observe(healed(&retired())));
    assert!(host.step(Time(0), join(5)).is_empty(), "a member: nothing");
    assert!(!host.serves(NEW));
    let no_segment = Input::Join {
        address: at(J, 20),
        set: vec![NodeId(11)],
        initial: Leader::Unset,
        copy: copy(5),
    };
    assert_eq!(host.step(Time(0), no_segment), Vec::new());
    joined(retired());
}

#[test]
fn a_node_runs_the_highest_copy_it_was_asked() {
    let mut host = joined(retired());
    assert!(host.step(Time(1), join(5)).is_empty(), "the same copy runs");
    assert!(
        host.step(Time(1), join(4)).is_empty(),
        "a lower copy is refused"
    );
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(5))
    );
    let effects = host.step(Time(1), join(6));
    assert_eq!(
        effects,
        vec![
            Effect::Stopped {
                address: NEW,
                why: Stop::Replaced
            },
            Effect::Read { address: NEW }
        ]
    );
    let durable = layout::replica_ops(
        NEW,
        PersistId(3),
        &[paros_journal::roles::ReplicaOp::Joining(Some(copy(5)))],
    );
    let entries: Vec<(Key, Entry)> = durable
        .into_iter()
        .map(|op| (op.key, Entry::Value(op.value)))
        .collect();
    let effects = host.step(
        Time(1),
        Input::Recovered {
            address: NEW,
            entries,
        },
    );
    let [Effect::Persist { id, .. }, ..] = effects.as_slice() else {
        panic!("the higher copy is on the disk first: {effects:?}");
    };
    assert_eq!(*id, PersistId(4), "the new batches follow the disk's");
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(6))
    );
}

#[test]
fn a_node_goes_on_with_a_higher_copy_on_its_disk() {
    let (host, effects) = join_over(7);
    assert!(
        !writes_a_copy(&effects),
        "no new copy is written: {effects:?}"
    );
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(7))
    );
}

/// The effects of a node asked copy 5 whose disk runs `on_disk`.
fn join_over(on_disk: u64) -> (Host, Vec<Effect>) {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    host.step(Time(0), join(5));
    let entries: Vec<(Key, Entry)> = layout::replica_ops(
        NEW,
        PersistId(2),
        &[paros_journal::roles::ReplicaOp::Joining(Some(copy(
            on_disk,
        )))],
    )
    .into_iter()
    .map(|op| (op.key, Entry::Value(op.value)))
    .collect();
    let effects = host.step(
        Time(0),
        Input::Recovered {
            address: NEW,
            entries,
        },
    );
    (host, effects)
}

fn writes_a_copy(effects: &[Effect]) -> bool {
    effects.iter().any(|e| {
        matches!(
            e,
            Effect::Persist { ops, .. } if ops.iter().any(|op| op.key.kind == Kind::Joining)
        )
    })
}

#[test]
fn a_node_goes_on_with_the_same_copy_on_its_disk() {
    let (host, effects) = join_over(5);
    assert!(
        !writes_a_copy(&effects),
        "the copy on the disk goes on, with no new start: {effects:?}"
    );
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(5))
    );
    let (host, effects) = join_over(4);
    assert!(writes_a_copy(&effects), "a lower copy on the disk ends");
    assert_eq!(
        host.replica(NEW).and_then(SegmentReplica::joining),
        Some(copy(5))
    );
}

/// `value` after one more controller election: copies of its term are
/// orphans.
fn next_term(value: &RegisterValue) -> RegisterValue {
    value
        .apply(&Change::Elect { holder: NodeId(30) })
        .expect("an election")
}

#[test]
fn a_join_of_a_term_that_is_over_is_refused() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(next_term(&retired())));
    assert!(host.step(Time(0), join(5)).is_empty(), "an orphan ask");
    assert!(!host.serves(NEW));
}

#[test]
fn a_copy_ends_when_its_term_is_over() {
    let mut host = joined(retired());
    let effects = host.step(Time(1), observe(next_term(&retired())));
    assert!(
        writes_a_copy(&effects),
        "the end is on the disk: {effects:?}"
    );
    assert!(
        effects.contains(&Effect::Stopped {
            address: NEW,
            why: Stop::Dropped
        }),
        "an orphan copy is dropped with no abandon"
    );
    assert!(!host.serves(NEW));
}

#[test]
fn a_join_read_after_the_term_changed_starts_no_copy() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    host.step(Time(0), join(5));
    host.step(Time(0), observe(next_term(&retired())));
    let effects = host.step(Time(0), recovered(NEW));
    assert!(
        !writes_a_copy(&effects),
        "no copy of a term that is over starts: {effects:?}"
    );
    assert!(
        effects.contains(&Effect::Stopped {
            address: NEW,
            why: Stop::Dropped
        }),
        "a node that no set names drops the segment: {effects:?}"
    );
}

#[test]
fn a_join_read_after_the_commit_starts_a_member() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    host.step(Time(0), join(5));
    host.step(Time(0), observe(healed(&retired())));
    host.step(Time(0), recovered(NEW));
    let replica = host.replica(NEW).expect("the replica runs");
    assert_eq!(replica.joining(), None, "a member runs no copy");
    assert!(replica.set().contains(&NodeId(20)));
}

#[test]
fn an_abandon_drops_a_copy_that_the_value_does_not_name() {
    let mut host = joined(retired());
    let other = host.step(
        Time(1),
        Input::Abandon {
            address: NEW,
            copy: copy(4),
        },
    );
    assert!(
        !other.iter().any(|e| matches!(e, Effect::Stopped { .. })),
        "an abandon of another copy keeps it"
    );
    let effects = host.step(
        Time(1),
        Input::Abandon {
            address: NEW,
            copy: copy(5),
        },
    );
    assert!(effects.contains(&Effect::Stopped {
        address: NEW,
        why: Stop::Dropped
    }));
    assert!(!host.serves(NEW));
}

#[test]
fn an_abandon_after_the_commit_keeps_a_member() {
    let mut host = joined(retired());
    host.step(Time(1), observe(healed(&retired())));
    let effects = host.step(
        Time(1),
        Input::Abandon {
            address: NEW,
            copy: copy(5),
        },
    );
    assert!(!effects.iter().any(|e| matches!(e, Effect::Stopped { .. })));
    assert!(host.serves(NEW));
}

#[test]
fn an_abandon_stops_a_copy_that_still_reads_its_disk() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    host.step(Time(0), join(5));
    assert_eq!(
        host.step(
            Time(0),
            Input::Abandon {
                address: NEW,
                copy: copy(4)
            }
        ),
        Vec::new()
    );
    assert!(host.serves(NEW), "another copy's abandon keeps the read");
    assert_eq!(
        host.step(
            Time(0),
            Input::Abandon {
                address: at(J, 20),
                copy: copy(5)
            }
        ),
        Vec::new()
    );
    let effects = host.step(
        Time(0),
        Input::Abandon {
            address: NEW,
            copy: copy(5),
        },
    );
    assert_eq!(
        effects,
        vec![Effect::Stopped {
            address: NEW,
            why: Stop::Dropped
        }]
    );
}

#[test]
fn an_abandon_of_a_member_that_reads_its_disk_keeps_it() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    host.step(Time(0), join(5));
    host.step(Time(0), observe(healed(&retired())));
    assert_eq!(
        host.step(
            Time(0),
            Input::Abandon {
                address: NEW,
                copy: copy(5)
            }
        ),
        Vec::new()
    );
    assert!(host.serves(NEW));
}

#[test]
fn a_holding_goes_to_the_replica_and_a_holds_to_its_asker() {
    let holding = Envelope {
        journal: J,
        from: NodeId(100),
        to: NodeId(11),
        message: Message::Holding {
            segment: SegmentId(1),
        },
    };
    assert_eq!(
        holding.receiver(),
        Address::replica(J, NodeId(11), SegmentId(1))
    );
    let holds = |to: u64| Envelope {
        journal: J,
        from: NodeId(11),
        to: NodeId(to),
        message: Message::Holds {
            segment: SegmentId(1),
            full: true,
        },
    };
    assert_eq!(holds(100).receiver(), at(J, 100), "a leader asked");

    // A new replica asked: the answer reaches it, and it pulls from the
    // member that holds the segment.
    let mut host = joined(retired());
    let effects = host.step(Time(1), Input::Message(holds(20)));
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::Send { envelope, .. }
                if envelope.to == NodeId(11) && matches!(envelope.message, Message::Pull { .. })
        )),
        "the new replica pulls from its source: {effects:?}"
    );
}

#[test]
fn a_holds_reaches_both_the_acceptor_and_the_new_replica_of_a_node() {
    let mut host = joined(retired());
    host.step(Time(1), acceptor(at(J, 20)));
    host.step(Time(1), recovered(at(J, 20)));
    let holds = Envelope {
        journal: J,
        from: NodeId(11),
        to: NodeId(20),
        message: Message::Holds {
            segment: SegmentId(1),
            full: true,
        },
    };
    let effects = host.step(Time(1), Input::Message(holds));
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::Send { envelope, .. }
                if envelope.to == NodeId(11) && matches!(envelope.message, Message::Pull { .. })
        )),
        "the new replica pulls although an acceptor serves the node: {effects:?}"
    );
}

#[test]
fn a_join_whose_set_names_the_node_is_refused() {
    let mut host = Host::new(CONFIG);
    host.step(Time(0), observe(retired()));
    let ask = Input::Join {
        address: NEW,
        set: vec![NodeId(11), NodeId(20)],
        initial: Leader::Unset,
        copy: copy(5),
    };
    assert_eq!(host.step(Time(0), ask), Vec::new());
    assert!(!host.serves(NEW));
}

#[test]
fn a_read_goes_to_its_replica_and_its_acks_come_back_to_it() {
    let mut host = Host::new(CONFIG);
    let replica = Address::replica(J, NodeId(11), SegmentId(1));
    let read = |from| Input::Read {
        address: replica,
        read: 7,
        from: Seq(from),
        limit: 4,
    };
    // A role that is not a replica that serves answers unserved.
    assert_eq!(
        host.step(Time(0), read(0)),
        vec![Effect::Output {
            address: replica,
            output: Output::ReadReply {
                read: 7,
                answer: ReadAnswer::Unserved,
            },
            after: None,
        }]
    );
    host.step(
        Time(0),
        Input::Assign(Assignment::Replica {
            address: at(J, 11),
            segment: SegmentId(1),
            set: vec![NodeId(11), NodeId(12), NodeId(13)],
            initial: Leader::Unset,
        }),
    );
    host.step(Time(0), recovered(replica));
    host.step(
        Time(0),
        Input::Observe {
            journal: J,
            value: value(),
        },
    );
    let mut tags = Vec::new();
    let asked: Vec<NodeId> = host
        .step(Time(1), read(0))
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Send { envelope, .. } => {
                let Message::PreRead { read: tag, .. } = envelope.message else {
                    panic!("a PreRead: {envelope:?}");
                };
                tags.push(tag);
                assert_eq!(envelope.receiver(), at(J, envelope.to.0));
                Some(envelope.to)
            }
            _ => None,
        })
        .collect();
    assert_eq!(asked, vec![NodeId(1), NodeId(2), NodeId(3)]);
    let tag = tags[0];
    assert!(tags.iter().all(|t| *t == tag), "one read, one tag");
    let ack = |from| Envelope {
        journal: J,
        from: NodeId(from),
        to: NodeId(11),
        message: Message::PreReadAck {
            segment: SegmentId(1),
            read: tag,
            watermark: None,
            bound: SegmentId(1),
        },
    };
    assert_eq!(ack(1).receiver(), replica, "an ack names its replica");
    assert_eq!(host.step(Time(2), Input::Message(ack(1))), Vec::new());
    let effects = host.step(Time(2), Input::Message(ack(2)));
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::Output {
                address,
                output: Output::ReadReply {
                    read: 7,
                    answer: ReadAnswer::Page { sealed: false, .. },
                },
                ..
            }] if *address == replica
        ),
        "{effects:?}"
    );
}

#[test]
fn a_proxy_sends_the_accepts_of_a_delegation_and_nacks_a_lower_one() {
    let mut host = Host::new(CONFIG);
    let proxy = at(J, 50);
    host.step(Time(0), Input::Assign(Assignment::Proxy { address: proxy }));
    assert!(host.serves(proxy), "a proxy needs no recovery");
    let delegate = |round| {
        Input::Message(Envelope {
            journal: J,
            from: NodeId(100 + round),
            to: NodeId(50),
            message: Message::Delegate {
                ballot: Ballot {
                    round,
                    node: NodeId(100 + round),
                },
                slot: Slot(0),
                command: Command::Noop,
                column: vec![NodeId(1), NodeId(2), NodeId(3)],
                segment: SegmentId(1),
                replicas: vec![NodeId(11), NodeId(12), NodeId(13)],
            },
        })
    };
    let sends = |effects: Vec<Effect>| -> Vec<Envelope> {
        effects
            .into_iter()
            .filter_map(|effect| match effect {
                Effect::Send { envelope, .. } => Some(envelope),
                _ => None,
            })
            .collect()
    };
    let accepts = sends(host.step(Time(1), delegate(2)));
    assert_eq!(
        accepts.iter().map(|e| e.to).collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );
    assert!(
        accepts.iter().all(|e| e.journal == J
            && e.from == NodeId(50)
            && matches!(e.message, Message::Accept { slot: Slot(0), .. })),
        "{accepts:?}"
    );
    let nack = sends(host.step(Time(2), delegate(1)));
    assert!(
        matches!(
            nack.as_slice(),
            [Envelope {
                to: NodeId(101),
                message: Message::Nack { .. },
                ..
            }]
        ),
        "a delegation below the proxy's ballot is answered Nack: {nack:?}"
    );
}
