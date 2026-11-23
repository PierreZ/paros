//! The **contract suites**: the checks that a backend keeps the contract
//! of its trait. A backend's own tests call them, for example:
//!
//! ```
//! use paros_journal_io::contract;
//! use paros_journal_io::mem::MemStorage;
//!
//! contract::storage(MemStorage::new);
//! contract::storage_reopen(MemStorage::new, |storage| storage);
//! ```
//!
//! Each suite panics, with the rule that failed, on the first break.
//!
//! **A suite certifies the API only** (Pierre, 2026-11-19). That a batch is
//! atomic across a crash, or durable once `sync` returned, a suite cannot
//! see on a healthy disk: crash safety is claimed only on a disk that can
//! inject faults (the moonpool-sim of the journal, §J7.4, #1330).

use paros_consensus::journal::JournalId;
use paros_journal::roles::{Message, PersistId};
use paros_journal::{NodeId, SegmentId, Slot};

use crate::clock::Clock;
use crate::family::Journal;
use crate::layout::{Entry, Key, Kind, Op};
use crate::storage::Storage;
use crate::transport::Transport;
use crate::wire::{Address, Envelope};

fn op(address: Address, kind: Kind, index: u64, value: &[u8]) -> Op {
    Op {
        key: Key::new(address, kind, index),
        value: value.to_vec(),
    }
}

fn values(entries: &[(Key, Entry)]) -> Vec<(Kind, u64, Vec<u8>)> {
    entries
        .iter()
        .map(|(key, entry)| match entry {
            Entry::Value(bytes) => (key.kind, key.index, bytes.clone()),
            Entry::Corrupt => panic!("storage: a healthy record reads as corrupt: {key:?}"),
        })
        .collect()
}

fn scan<S: Storage<Journal>>(storage: &mut S, address: Address) -> Vec<(Key, Entry)> {
    let entries = storage
        .scan(address)
        .unwrap_or_else(|error| panic!("storage: a scan of a healthy disk fails: {error}"));
    assert!(
        entries.iter().all(|(key, _)| key.address == address),
        "storage: a scan gives only the records of its role"
    );
    assert!(
        entries
            .windows(2)
            .all(|pair| pair[0].0.to_bytes() < pair[1].0.to_bytes()),
        "storage: a scan gives each key once, in the order of its bytes"
    );
    entries
}

fn commit<S: Storage<Journal>>(storage: &mut S, ops: &[Op], sync: bool) {
    storage
        .commit(ops, sync)
        .unwrap_or_else(|error| panic!("storage: a commit on a healthy disk fails: {error}"));
}

/// The contract of [`Storage`] on fresh disks that `fresh` makes: a
/// commit is seen by every later scan, the ops of a commit apply in order,
/// a scan gives one role's records in key order and no other record, and
/// a fresh disk is empty.
///
/// # Panics
///
/// On the first rule that the backend breaks.
pub fn storage<S: Storage<Journal>>(mut fresh: impl FnMut() -> S) {
    let a = Address::new(JournalId(1), NodeId(1));
    // A node whose bytes share a prefix with `a`'s node, in another journal.
    let b = Address::new(JournalId(256), NodeId(1));
    let c = Address::new(JournalId(1), NodeId(256));

    let mut disk = fresh();
    for address in [a, b, c] {
        assert!(
            scan(&mut disk, address).is_empty(),
            "storage: a fresh disk is empty"
        );
    }

    // One commit across three roles; slots out of order.
    commit(
        &mut disk,
        &[
            op(a, Kind::Vote, 10, b"ten"),
            op(b, Kind::Vote, 1, b"other journal"),
            op(a, Kind::Vote, 2, b"two"),
            op(c, Kind::Chosen, 3, b"other node"),
            op(a, Kind::Promise, 0, b"promise"),
            op(a, Kind::Durable, 0, b"d0"),
        ],
        true,
    );
    assert_eq!(
        values(&scan(&mut disk, a)),
        vec![
            (Kind::Durable, 0, b"d0".to_vec()),
            (Kind::Promise, 0, b"promise".to_vec()),
            (Kind::Vote, 2, b"two".to_vec()),
            (Kind::Vote, 10, b"ten".to_vec()),
        ],
        "storage: a scan gives the role's records of a commit, in key order"
    );
    assert_eq!(
        values(&scan(&mut disk, b)),
        vec![(Kind::Vote, 1, b"other journal".to_vec())],
        "storage: the roles of two journals do not mix"
    );
    assert_eq!(
        values(&scan(&mut disk, c)),
        vec![(Kind::Chosen, 3, b"other node".to_vec())],
        "storage: two roles of one journal do not mix"
    );

    // In one commit, the later op at a key wins; a later commit overwrites.
    commit(
        &mut disk,
        &[
            op(a, Kind::Durable, 0, b"d1"),
            op(a, Kind::Vote, 2, b"two again"),
            op(a, Kind::Durable, 0, b"d2"),
        ],
        false,
    );
    assert_eq!(
        values(&scan(&mut disk, a)),
        vec![
            (Kind::Durable, 0, b"d2".to_vec()),
            (Kind::Promise, 0, b"promise".to_vec()),
            (Kind::Vote, 2, b"two again".to_vec()),
            (Kind::Vote, 10, b"ten".to_vec()),
        ],
        "storage: the later op at a key wins, in a commit and across commits"
    );

    // An empty commit changes nothing.
    commit(&mut disk, &[], true);
    assert_eq!(
        scan(&mut disk, a).len(),
        4,
        "storage: an empty commit changes nothing"
    );

    // A value may be empty, and large.
    let large = vec![7; 1 << 20];
    commit(
        &mut disk,
        &[op(c, Kind::Base, 0, b""), op(c, Kind::Chosen, 4, &large)],
        true,
    );
    assert_eq!(
        values(&scan(&mut disk, c)),
        vec![
            (Kind::Base, 0, Vec::new()),
            (Kind::Chosen, 3, b"other node".to_vec()),
            (Kind::Chosen, 4, large),
        ],
        "storage: a value may be empty or large"
    );
}

/// The contract of [`Storage`] across a reopen: every commit made with
/// `sync` before `reopen` is seen after it. `reopen` closes the disk and
/// opens it again, with no crash.
///
/// # Panics
///
/// On the first rule that the backend breaks.
pub fn storage_reopen<S: Storage<Journal>>(
    mut fresh: impl FnMut() -> S,
    mut reopen: impl FnMut(S) -> S,
) {
    let a = Address::new(JournalId(7), NodeId(3));
    let mut disk = fresh();
    commit(
        &mut disk,
        &[
            op(a, Kind::Promise, 0, b"p"),
            op(a, Kind::Vote, 0, b"v0"),
            op(a, Kind::Durable, 0, &PersistId(0).0.to_be_bytes()),
        ],
        true,
    );
    commit(&mut disk, &[op(a, Kind::Vote, 1, b"v1")], true);
    let before = values(&scan(&mut disk, a));
    let mut disk = reopen(disk);
    assert_eq!(
        values(&scan(&mut disk, a)),
        before,
        "storage: a synced commit is seen after a reopen"
    );
}

/// The contract of [`Transport`] between two processes: `sender` and
/// `receiver`, where `receiver` serves the role at `to`. Every message
/// that arrives is one that was sent, to the node that it names (the
/// `Host` of that node routes it to the role); nothing arrives that was not
/// sent. `settle` lets the network move (a real
/// network takes time; one in memory needs nothing).
///
/// A message may be lost, repeated or reordered, so this suite does not
/// ask that a message arrive.
///
/// # Panics
///
/// On the first rule that the backend breaks.
pub fn transport<T: Transport<Journal>>(
    sender: &mut T,
    receiver: &mut T,
    to: Address,
    mut settle: impl FnMut(),
) {
    settle();
    assert!(
        receiver.receive().is_none(),
        "transport: nothing arrives that was not sent"
    );
    let sent: Vec<Envelope> = (0..16)
        .map(|i| Envelope {
            journal: to.journal,
            from: NodeId(i),
            to: to.node,
            message: Message::Pull {
                segment: SegmentId(i + 1),
                from: Slot(i),
            },
        })
        .collect();
    for envelope in &sent {
        sender.send(envelope.clone());
    }
    settle();
    while let Some(envelope) = receiver.receive() {
        assert!(
            sent.contains(&envelope),
            "transport: a message arrives as it was sent: {envelope:?}"
        );
        assert_eq!(
            envelope.receiver().node_address(),
            to.node_address(),
            "transport: a message arrives at the node it names"
        );
    }
}

/// The contract of [`Clock`]: the time never goes back. `wait` lets time
/// pass between two reads.
///
/// # Panics
///
/// When the time goes back.
pub fn clock<C: Clock>(clock: &C, mut wait: impl FnMut()) {
    let mut last = clock.now();
    for _ in 0..64 {
        wait();
        let now = clock.now();
        assert!(now >= last, "clock: the time never goes back");
        last = now;
    }
}

#[cfg(test)]
mod tests;
