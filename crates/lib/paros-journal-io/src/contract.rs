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
//! The register suite takes a register that holds one journal:
//!
//! ```
//! use paros_journal::register::RegisterValue;
//! use paros_journal_io::contract;
//! use paros_journal_io::mem::MemRegister;
//!
//! contract::register(|journal| {
//!     let register = MemRegister::new();
//!     register.create(journal, RegisterValue::new());
//!     register
//! });
//! ```
//!
//! Each suite panics, with the rule that failed, on the first break.
//!
//! **A suite certifies the API only** (Pierre, 2026-11-19). That a batch is
//! atomic across a crash, or durable once `commit` returned, a suite cannot
//! see on a healthy disk: crash safety is claimed only on a disk that can
//! inject faults (the moonpool-sim of the journal, §J7.4, #1330).

use paros_consensus::journal::JournalId;
use paros_journal::register::{Ballot, Change, RegisterValue};
use paros_journal::roles::{Message, PersistId};
use paros_journal::{
    BoundaryState, Grid, Leader, NodeId, OpeningCall, RELEASE, SegmentId, Seq, Slot,
};

use crate::clock::Clock;
use crate::family::Journal;
use crate::layout::{Entry, Key, Kind, Op};
use crate::register::{ChangeError, Register};
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

fn list<S: Storage<Journal>>(storage: &mut S) -> Vec<Address> {
    let roles = storage
        .list()
        .unwrap_or_else(|error| panic!("storage: a list of a healthy disk fails: {error}"));
    assert!(
        roles.windows(2).all(|pair| pair[0] < pair[1]),
        "storage: a list names each role once, in address order"
    );
    roles
}

fn commit<S: Storage<Journal>>(storage: &mut S, ops: &[Op]) {
    storage
        .commit(ops)
        .unwrap_or_else(|error| panic!("storage: a commit on a healthy disk fails: {error}"));
}

/// A list names the roles `with` that have records, and then `r` too once
/// a commit gave it one: a replica apart from its node's acceptor.
fn lists<S: Storage<Journal>>(disk: &mut S, with: [Address; 3], r: Address) {
    let mut roles = with.to_vec();
    roles.sort();
    assert_eq!(
        list(disk),
        roles,
        "storage: a list names each role that has records, and no other"
    );
    commit(disk, &[op(r, Kind::Durable, 0, b"r")]);
    roles.push(r);
    roles.sort();
    assert_eq!(
        list(disk),
        roles,
        "storage: a list names a replica apart from its node's acceptor"
    );
}

/// The contract of [`Storage`] on fresh disks that `fresh` makes: a
/// commit is seen by every later scan, the ops of a commit apply in order,
/// a scan gives one role's records in key order and no other record, a
/// list names each role with records once, in address order, and a fresh
/// disk is empty.
///
/// # Panics
///
/// On the first rule that the backend breaks.
pub fn storage<S: Storage<Journal>>(mut fresh: impl FnMut() -> S) {
    let a = Address::new(JournalId(1), NodeId(1));
    // A node whose bytes share a prefix with `a`'s node, in another journal.
    let b = Address::new(JournalId(256), NodeId(1));
    let c = Address::new(JournalId(1), NodeId(256));

    // A replica of `a`'s node: a role of its own, with its own records.
    let r = Address::replica(JournalId(1), NodeId(1), SegmentId(2));

    let mut disk = fresh();
    for address in [a, b, c, r] {
        assert!(
            scan(&mut disk, address).is_empty(),
            "storage: a fresh disk is empty"
        );
    }
    assert!(
        list(&mut disk).is_empty(),
        "storage: a fresh disk lists no role"
    );

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
    assert!(
        scan(&mut disk, r).is_empty(),
        "storage: a replica of a node does not see the node's acceptor"
    );
    lists(&mut disk, [a, b, c], r);

    // In one commit, the later op at a key wins; a later commit overwrites.
    commit(
        &mut disk,
        &[
            op(a, Kind::Durable, 0, b"d1"),
            op(a, Kind::Vote, 2, b"two again"),
            op(a, Kind::Durable, 0, b"d2"),
        ],
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
    commit(&mut disk, &[]);
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

/// The contract of [`Storage`] across a reopen: every commit made before
/// `reopen` is seen after it. `reopen` closes the disk and
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
    );
    commit(&mut disk, &[op(a, Kind::Vote, 1, b"v1")]);
    let before = values(&scan(&mut disk, a));
    let listed = list(&mut disk);
    assert_eq!(listed, vec![a], "storage: a list names the one role");
    let mut disk = reopen(disk);
    assert_eq!(
        values(&scan(&mut disk, a)),
        before,
        "storage: a commit is seen after a reopen"
    );
    assert_eq!(
        list(&mut disk),
        listed,
        "storage: a list after a reopen names the same roles"
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

/// The controller's call at `term` that opens the segment after `expected`.
fn opening(term: u64, expected: Option<SegmentId>) -> Change {
    Change::Reshape {
        opening: OpeningCall {
            term,
            expected,
            grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]])
                .expect("register: a v1 grid"),
            replicas: vec![NodeId(11), NodeId(12), NodeId(13)],
        },
        release: RELEASE,
    }
}

/// The changes of the register suite, in order: each kind of change that
/// the journal's roles and controller ask but the trim, refusals of the
/// term, bind, unretired, seal and retire guards, and each change that
/// the journal asks again (a bind, a reshape, a seal, a boundary state, a
/// retire) twice in a row, the second over its own done write.
fn register_script() -> Vec<Change> {
    let s1 = SegmentId(1);
    let seal = |end| Change::Seal {
        segment: s1,
        end: Slot(end),
    };
    let boundary = Change::Boundary {
        segment: s1,
        state: BoundaryState {
            next_seq: Seq(3),
            first_seq: Seq(0),
            leader: Leader::Unset,
        },
    };
    let retire = |holders| Change::Retire {
        segment: s1,
        holders,
        unrepaired_damage: false,
    };
    vec![
        Change::Elect { holder: NodeId(1) },
        opening(1, None),
        Change::Bind {
            ballot: Ballot {
                round: 1,
                node: NodeId(21),
            },
        },
        Change::Bind {
            ballot: Ballot {
                round: 1,
                node: NodeId(21),
            },
        },
        Change::Elect { holder: NodeId(2) },
        opening(1, Some(s1)),
        opening(2, Some(s1)),
        opening(2, Some(s1)),
        Change::Bind {
            ballot: Ballot {
                round: 1,
                node: NodeId(20),
            },
        },
        Change::Bind {
            ballot: Ballot {
                round: 2,
                node: NodeId(21),
            },
        },
        retire(2),
        seal(4),
        seal(4),
        seal(5),
        boundary.clone(),
        boundary,
        retire(1),
        retire(2),
        retire(2),
    ]
}

/// The contract of [`Register`] on a fresh register that `fresh(journal)`
/// makes, which holds `journal` at [`RegisterValue::new`] and no other
/// journal. On a healthy register every change answers: a change whose
/// guard holds is done and gives the value that
/// [`RegisterValue::apply`] gives, a change whose guard fails is
/// [`ChangeError::Refused`] with the guard's refusal and changes nothing, a
/// change asked again over its own done write is done again (§J3.4, "a
/// change asked again"), a read gives the value of the last done write, and
/// a journal that the register does not hold is [`ChangeError::Unknown`]
/// and reads `None`. The suite runs the same changes on a value of its own
/// and compares each answer with it.
///
/// # Panics
///
/// On the first answer or read that breaks the contract, with the rule.
pub fn register<R: Register>(mut fresh: impl FnMut(JournalId) -> R) {
    let journal = JournalId(7);
    let other = JournalId(8);
    let mut register = fresh(journal);
    let mut value = RegisterValue::new();
    assert_eq!(
        register.read(journal),
        Some(value.clone()),
        "register: a fresh register reads its start value"
    );
    assert_eq!(
        register.read(other),
        None,
        "register: a journal that the register does not hold reads none"
    );
    assert_eq!(
        register.change(other, &Change::Elect { holder: NodeId(1) }),
        Err(ChangeError::Unknown),
        "register: a change of a journal that the register does not hold is unknown"
    );
    assert_eq!(
        register.read(other),
        None,
        "register: a change does not create a register"
    );
    let mut done = 0;
    let mut refused = 0;
    let mut last: Option<Change> = None;
    for change in register_script() {
        let answer = register.change(journal, &change);
        if last.as_ref() == Some(&change) {
            assert!(
                answer.is_ok(),
                "register: a change asked again over its own done write is done again: {change:?} gave {answer:?}"
            );
        }
        last = Some(change.clone());
        match value.apply(&change) {
            Ok(next) => {
                assert_eq!(
                    answer,
                    Ok(next.clone()),
                    "register: a change whose guard holds is done with the applied value: {change:?}"
                );
                value = next;
                done += 1;
            }
            Err(refusal) => {
                assert_eq!(
                    answer,
                    Err(ChangeError::Refused(refusal)),
                    "register: a change whose guard fails is refused with its guard: {change:?}"
                );
                refused += 1;
            }
        }
        assert_eq!(
            register.read(journal),
            Some(value.clone()),
            "register: a read gives the value of the last done write, after {change:?}"
        );
    }
    assert!(done > 0, "register: the suite runs done changes");
    assert!(refused > 0, "register: the suite runs refused changes");
    assert_eq!(
        register.read(other),
        None,
        "register: the changes of one journal create no other"
    );
}

#[cfg(test)]
mod tests;
