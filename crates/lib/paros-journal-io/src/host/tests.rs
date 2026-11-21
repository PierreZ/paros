//! Tests of the `Host`: assignments, recovery from records, routing by
//! journal, the flight rule on the effects, calls and the timer.

use paros_journal::register::Ballot;
use paros_journal::roles::{LeaderPhase, Message};
use paros_journal::{Grid, OpeningCall, RELEASE, Slot};

use super::*;
use crate::layout::{Kind, acceptor_ops};

const J: JournalId = JournalId(1);
const K: JournalId = JournalId(2);
const CONFIG: HostConfig = HostConfig {
    tick: 10,
    resend_page: 4,
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
        },
    })
}

#[test]
fn an_acceptor_starts_with_a_read_and_routes_by_journal() {
    let mut host = Host::new(CONFIG);
    let effects = host.step(Time(5), acceptor(at(J, 1)));
    assert_eq!(
        effects,
        vec![
            Effect::SetTimer { at: Time(15) },
            Effect::Read { address: at(J, 1) }
        ]
    );
    assert!(host.serves(at(J, 1)));
    assert!(host.acceptor(at(J, 1)).is_none(), "it still recovers");
    assert!(
        host.step(Time(6), prepare(J, 1)).is_empty(),
        "a role that recovers loses its messages"
    );
    assert_eq!(host.step(Time(6), recovered(at(J, 1))), Vec::new());
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
        Effect::Send { envelope, after },
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
        &[paros_journal::roles::AcceptorOp::SetPromise(promised)],
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
        .filter(|e| matches!(e, Effect::Send { envelope, after: None } if matches!(envelope.message, Message::Prepare { .. })))
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
            vec![Effect::Reply {
                address,
                call: id,
                outcome: CallOutcome::NotLeader,
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
fn the_timer_ticks_every_role_and_arms_again() {
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
    assert!(effects.contains(&Effect::SetTimer { at: Time(10) }));
    host.step(Time(0), recovered(replica));
    let base = host.replica(replica).and_then(SegmentReplica::front);
    assert_eq!(
        base,
        Some(Slot(0)),
        "the replica took its base from the value"
    );
    let effects = host.step(Time(10), Input::TimerFired);
    let pulls = effects
        .iter()
        .filter(|e| matches!(e, Effect::Send { envelope, .. } if matches!(envelope.message, Message::Pull { .. })))
        .count();
    assert_eq!(pulls, 2, "the replica asks its two peers");
    assert_eq!(effects.last(), Some(&Effect::SetTimer { at: Time(20) }));
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
        host.step(Time(0), recovered(at(journal, 11)));
    }
    assert_eq!(
        host.addresses().collect::<Vec<_>>(),
        vec![at(J, 11), at(K, 11)]
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
