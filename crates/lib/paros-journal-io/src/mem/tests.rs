//! The backends in memory keep the contracts of their traits.

use paros_journal::NodeId;
use paros_journal::register::{Change, RegisterValue};

use super::*;
use crate::contract;

#[test]
fn mem_storage_keeps_the_storage_contract() {
    contract::storage(MemStorage::new);
    contract::storage_reopen(MemStorage::new, |storage| storage);
}

#[test]
fn mem_storage_reports_a_corrupt_record_and_a_planted_failure() {
    let address = Address::new(JournalId(1), NodeId(2));
    let key = Key::new(address, crate::layout::Kind::Promise, 0);
    let mut storage = MemStorage::new();
    assert!(storage.is_empty());
    let op = Op {
        key,
        value: vec![1],
    };
    storage.commit(std::slice::from_ref(&op)).expect("a commit");
    storage.corrupt(key);
    assert_eq!(
        storage.scan(address).expect("a scan"),
        vec![(key, Entry::Corrupt)]
    );
    storage.commit(&[op]).expect("a commit");
    assert_eq!(
        storage.scan(address).expect("a scan"),
        vec![(key, Entry::Value(vec![1]))],
        "a new write heals the record"
    );
    storage.fail_next_commit(StorageError::Full);
    let other = Op {
        key: Key::new(address, crate::layout::Kind::Vote, 1),
        value: vec![2],
    };
    let error = storage
        .commit(std::slice::from_ref(&other))
        .expect_err("a planted failure");
    assert_eq!(error, StorageError::Full);
    assert_eq!(storage.len(), 1, "a failed commit writes nothing");
    storage
        .commit(&[other])
        .expect("only the next commit fails");
    assert_eq!(storage.len(), 2);
    assert!(!storage.is_empty());
}

#[test]
fn mem_storage_lists_a_role_that_lost_a_record_and_its_scan_reports_the_loss() {
    let address = Address::new(JournalId(1), NodeId(2));
    let other = Address::new(JournalId(1), NodeId(3));
    let key = Key::new(address, crate::layout::Kind::Promise, 0);
    let mut storage = MemStorage::new();
    storage
        .commit(&[
            Op {
                key,
                value: vec![1],
            },
            Op {
                key: Key::new(other, crate::layout::Kind::Promise, 0),
                value: vec![2],
            },
        ])
        .expect("a commit");
    storage.lose(key);
    assert_eq!(storage.len(), 1, "the record is gone");
    assert_eq!(
        storage.list().expect("a list"),
        vec![address, other],
        "faulty, not empty: the list still names the role"
    );
    assert_eq!(storage.scan(address), Err(StorageError::Lost));
    assert_eq!(storage.scan(address), Err(StorageError::Lost), "each time");
    assert_eq!(storage.scan(other).expect("a scan").len(), 1);
}

#[test]
fn mem_network_keeps_the_transport_contract() {
    let network = MemNetwork::new();
    let mut a = network.endpoint();
    let mut b = network.endpoint();
    let to = Address::new(JournalId(5), NodeId(1));
    b.route(to);
    contract::transport(&mut a, &mut b, to, || {});
    assert_eq!(network.in_flight(), 0, "the suite read every message");
    a.send(Envelope {
        journal: to.journal,
        from: NodeId(9),
        to: to.node,
        message: paros_journal::roles::Message::Pull {
            segment: paros_journal::SegmentId(1),
            from: paros_journal::Slot(0),
        },
    });
    assert_eq!(network.in_flight(), 1);
    assert!(a.receive().is_none(), "it went to the other endpoint");
    assert!(b.receive().is_some());
}

#[test]
fn mem_network_loses_a_message_to_a_role_nobody_serves() {
    let network = MemNetwork::new();
    let mut a = network.endpoint();
    a.send(Envelope {
        journal: JournalId(1),
        from: NodeId(1),
        to: NodeId(2),
        message: paros_journal::roles::Message::Pull {
            segment: paros_journal::SegmentId(1),
            from: paros_journal::Slot(0),
        },
    });
    assert_eq!(network.in_flight(), 0);
    assert!(a.receive().is_none());
}

#[test]
fn manual_clock_keeps_the_clock_contract() {
    let clock = ManualClock::new();
    let mover = clock.clone();
    contract::clock(&clock, || mover.advance(5));
    assert_eq!(clock.now(), Time(5 * 64), "the copies share the time");
}

#[test]
fn mem_register_runs_a_change_or_refuses_it() {
    let journal = JournalId(4);
    let mut register = MemRegister::new();
    assert_eq!(register.read(journal), None);
    register.create(journal, RegisterValue::new());
    let elect = Change::Elect { holder: NodeId(0) };
    let value = register
        .change(journal, &elect)
        .expect("the first election");
    assert_eq!(register.read(journal), Some(value.clone()));
    let mut copy = register.clone();
    assert_eq!(
        copy.read(journal),
        Some(value),
        "the copies share the value"
    );
    let unknown = Change::Seal {
        segment: paros_journal::SegmentId(9),
        end: paros_journal::Slot(1),
    };
    let before = register.value(journal);
    assert!(register.change(journal, &unknown).is_err(), "no segment 9");
    assert_eq!(register.value(journal), before, "a refusal changes nothing");
}

#[test]
fn mem_register_refuses_a_change_of_a_journal_it_does_not_hold() {
    let mut register = MemRegister::new();
    let elect = Change::Elect { holder: NodeId(0) };
    assert!(
        register.change(JournalId(5), &elect).is_err(),
        "no register for journal 5"
    );
    assert_eq!(
        register.read(JournalId(5)),
        None,
        "a change does not create a register"
    );
}

#[test]
fn mem_register_loses_a_planted_request_or_reply() {
    let journal = JournalId(4);
    let mut register = MemRegister::new();
    register.create(journal, RegisterValue::new());
    let elect = Change::Elect { holder: NodeId(0) };
    register.lose_next(false);
    assert_eq!(
        register.change(journal, &elect),
        Err(ChangeError::NoAnswer),
        "a lost request"
    );
    assert_eq!(
        register.read(journal),
        Some(RegisterValue::new()),
        "a lost request runs nothing"
    );
    register.lose_next(true);
    assert_eq!(
        register.change(journal, &elect),
        Err(ChangeError::NoAnswer),
        "a lost reply"
    );
    assert_ne!(
        register.read(journal),
        Some(RegisterValue::new()),
        "a lost reply hides a done write"
    );
}
