//! Tests of the on-disk format: keys sort as their bytes, every batch of a
//! role reads back as the disk that the role's own `apply` gives, and every
//! damaged record is reported.

use paros_journal::fold::BoundaryState;
use paros_journal::roles::{AcceptorDisk, AcceptorOp, AcceptorWrite, ReplicaDisk, ReplicaOp};
use paros_journal::{Command, Leader};

use super::*;
use crate::mem::MemStorage;
use crate::storage::Storage;

const A: Address = Address {
    journal: JournalId(3),
    node: NodeId(9),
    segment: None,
};

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(100),
    }
}

#[test]
fn key_bytes_sort_as_keys_and_read_back() {
    let mut keys = Vec::new();
    for journal in [0, 1, 255, 256, u64::MAX] {
        for node in [0, 1, 256] {
            for segment in [None, Some(1), Some(256), Some(u64::MAX)] {
                let address = Address {
                    journal: JournalId(journal),
                    node: NodeId(node),
                    segment: segment.map(SegmentId),
                };
                for kind in Kind::ALL {
                    for index in [0, 1, 255, 256, u64::MAX] {
                        keys.push(Key::new(address, kind, index));
                    }
                }
            }
        }
    }
    let mut by_bytes = keys.clone();
    by_bytes.sort_by_key(Key::to_bytes);
    keys.sort();
    assert_eq!(keys, by_bytes, "the bytes sort as the key does");
    for key in &keys {
        assert_eq!(Key::from_bytes(&key.to_bytes()), Some(*key));
        assert!(key.to_bytes().starts_with(&Key::prefix(key.address)));
    }
    assert_eq!(Key::from_bytes(&[0; KEY_LEN - 1]), None, "a short key");
    let mut bad = Key::new(A, Kind::Vote, 1).to_bytes();
    bad[PREFIX_LEN] = 99;
    assert_eq!(Key::from_bytes(&bad), None, "an unknown kind");
}

fn commit(storage: &mut MemStorage, ops: &[Op]) {
    storage.commit(ops, true).expect("a healthy disk");
}

#[test]
fn acceptor_batches_read_back_as_the_roles_disk() {
    let batches: Vec<(PersistId, Vec<AcceptorOp>)> = vec![
        (
            PersistId(0),
            vec![AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(1)))],
        ),
        (
            PersistId(1),
            vec![
                AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(2))),
                AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                    slot: Slot(0),
                    ballot: ballot(2),
                    value: Command::Noop,
                }),
            ],
        ),
        (
            PersistId(2),
            vec![AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                slot: Slot(7),
                ballot: ballot(2),
                value: Command::SetLeader {
                    new: None,
                    old: Leader::Unset,
                },
            })],
        ),
    ];
    let mut storage = MemStorage::new();
    let mut expected = AcceptorDisk::default();
    for (id, ops) in &batches {
        commit(&mut storage, &acceptor_ops(A, *id, ops));
        expected.apply(*id, ops);
        let read = decode_acceptor(A, &storage.scan(A).expect("a scan"));
        assert_eq!(read, Ok(expected.clone()), "after batch {id:?}");
    }
}

#[test]
fn replica_batches_read_back_as_the_roles_disk() {
    let boundary = BoundaryState::fresh(Leader::Unset);
    let batches: Vec<(PersistId, Vec<ReplicaOp>)> = vec![
        (
            PersistId(4),
            vec![
                ReplicaOp::Base {
                    boundary,
                    start: Slot(3),
                },
                ReplicaOp::Chosen {
                    slot: Slot(3),
                    command: Command::Noop,
                },
            ],
        ),
        (
            PersistId(5),
            vec![
                ReplicaOp::Joining(Some(paros_journal::roles::CopyId { term: 3, number: 9 })),
                ReplicaOp::Chosen {
                    slot: Slot(4),
                    command: Command::Noop,
                },
            ],
        ),
        (PersistId(6), vec![ReplicaOp::Joining(None)]),
    ];
    let mut storage = MemStorage::new();
    let mut expected = ReplicaDisk::default();
    for (id, ops) in &batches {
        commit(&mut storage, &replica_ops(A, *id, ops));
        expected.apply(*id, ops);
        let read = decode_replica(A, &storage.scan(A).expect("a scan"));
        assert_eq!(read, Ok(expected.clone()), "after batch {id:?}");
    }
}

#[test]
fn every_damaged_record_is_reported() {
    let mut storage = MemStorage::new();
    let ops = acceptor_ops(
        A,
        PersistId(0),
        &[
            AcceptorOp::Write(AcceptorWrite::SetPromise(ballot(1))),
            AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                slot: Slot(1),
                ballot: ballot(1),
                value: Command::Noop,
            }),
            AcceptorOp::Write(AcceptorWrite::AppendAccepted {
                slot: Slot(2),
                ballot: ballot(1),
                value: Command::Noop,
            }),
        ],
    );
    commit(&mut storage, &ops);
    let corrupt = Key::new(A, Kind::Vote, 1);
    let garbled = Key::new(A, Kind::Vote, 2);
    let foreign = Key::new(A, Kind::Chosen, 5);
    storage.corrupt(corrupt);
    commit(
        &mut storage,
        &[
            Op {
                key: garbled,
                value: vec![1, 2, 3],
            },
            Op {
                key: foreign,
                value: Vec::new(),
            },
        ],
    );
    let entries = storage.scan(A).expect("a scan");
    assert_eq!(
        decode_acceptor(A, &entries),
        Err(Damage {
            keys: vec![corrupt, garbled, foreign]
        })
    );
    let replica = decode_replica(A, &entries).expect_err("an acceptor's records");
    assert!(replica.keys.contains(&Key::new(A, Kind::Promise, 0)));
    let other = Address::new(JournalId(3), NodeId(10));
    assert_eq!(
        decode_acceptor(other, &entries).map_err(|d| d.keys.len()),
        Err(entries.len()),
        "every record of another role is damage"
    );
}

#[test]
fn the_replicas_of_one_node_are_two_ranges() {
    let mut storage = MemStorage::new();
    let first = Address::replica(JournalId(3), NodeId(9), SegmentId(1));
    let second = Address::replica(JournalId(3), NodeId(9), SegmentId(2));
    commit(
        &mut storage,
        &[
            Op {
                key: Key::new(first, Kind::Base, 0),
                value: vec![1],
            },
            Op {
                key: Key::new(second, Kind::Base, 0),
                value: vec![2],
            },
        ],
    );
    let mut scan = |address| storage.scan(address).expect("a scan");
    assert_eq!(
        scan(first),
        vec![(Key::new(first, Kind::Base, 0), Entry::Value(vec![1]))]
    );
    assert_eq!(
        scan(second),
        vec![(Key::new(second, Kind::Base, 0), Entry::Value(vec![2]))]
    );
    assert_eq!(scan(A), Vec::new(), "the node's own range holds no replica");
}
