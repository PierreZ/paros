//! The backends of the walks that run the generic driver of
//! `paros-journal-io` (the driver walk, [`crate::host`], and the whole
//! journal walk, [`crate::whole`]): a disk that survives its process and
//! fails commits inside the contract, and a transport that sends into the
//! walk's mailbox and judges the flight rule at the wire.
//!
//! Both are the walk's, never the driver's: a fault they inject stays inside
//! the contract of `paros_journal_io::Storage` (a failed commit may or may
//! not have written its batch; a commit that returned `Ok` is durable).

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use paros_journal::register::Ballot;
use paros_journal::roles::Message;
use paros_journal_io::mem::MemStorage;
use paros_journal_io::{Address, Entry, Envelope, Key, Kind, Op, Storage, StorageError, Transport};

/// The faults that a walk arms on a disk, for its next commit.
#[derive(Clone, Copy, Debug, Default)]
pub struct Faults {
    /// The commit fails and writes nothing.
    pub fail_before: bool,
    /// The commit writes its batch, then fails.
    pub fail_after: bool,
    /// A planted bug: the commit writes nothing and says it is done.
    pub lie: bool,
}

/// The disk of one process: it survives the process. `seen` looks at each
/// batch that a commit holds, for the reach sites of a walk.
#[derive(Clone)]
pub struct Disk {
    /// The records.
    pub store: Rc<RefCell<MemStorage>>,
    /// The faults armed for the next commit.
    pub faults: Rc<RefCell<Faults>>,
    seen: fn(&[Op]),
}

impl Disk {
    /// An empty disk.
    #[must_use]
    pub fn new(seen: fn(&[Op])) -> Self {
        Self {
            store: Rc::default(),
            faults: Rc::default(),
            seen,
        }
    }

    /// The value at `key`, if the disk holds one.
    ///
    /// # Panics
    ///
    /// When a scan of the memory fails: a scan of memory never fails.
    #[must_use]
    pub fn record(&self, key: Key) -> Option<Vec<u8>> {
        let entries = self
            .store
            .borrow_mut()
            .scan(key.address)
            .expect("a scan of memory");
        entries.into_iter().find_map(|(k, entry)| match entry {
            Entry::Value(bytes) if k == key => Some(bytes),
            _ => None,
        })
    }

    /// Lose every record: a planted bug, never a fault of the contract.
    pub fn wipe(&self) {
        *self.store.borrow_mut() = MemStorage::new();
    }

    /// Lose the records of every acceptor: a planted bug, never a fault of
    /// the contract. The replicas keep theirs.
    pub fn wipe_acceptors(&self) {
        self.store
            .borrow_mut()
            .lose(|key| key.address.segment.is_none());
    }

    /// Arm a fault of the contract for the next commit: it fails before or
    /// after its write, as `after` says.
    pub fn arm(&self, after: bool) {
        let mut faults = self.faults.borrow_mut();
        if after {
            faults.fail_after = true;
        } else {
            faults.fail_before = true;
        }
    }

    /// Arm the planted bug: the next commit writes nothing and says it is
    /// done.
    pub fn arm_lie(&self) {
        self.faults.borrow_mut().lie = true;
    }
}

impl Storage for Disk {
    fn commit(&mut self, ops: &[Op], sync: bool) -> Result<(), StorageError> {
        (self.seen)(ops);
        let faults = std::mem::take(&mut *self.faults.borrow_mut());
        if faults.lie {
            return Ok(());
        }
        if faults.fail_before {
            return Err(StorageError("the walk failed the commit".into()));
        }
        self.store.borrow_mut().commit(ops, sync)?;
        if faults.fail_after {
            return Err(StorageError(
                "the walk failed the commit after its write".into(),
            ));
        }
        Ok(())
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        self.store.borrow_mut().scan(address)
    }
}

/// The bytes of a ballot as the layout writes it at the head of a promise
/// and of a vote: the round, then the node, big-endian.
fn ballot_bytes(ballot: Ballot) -> Vec<u8> {
    let mut bytes = ballot.round.to_be_bytes().to_vec();
    bytes.extend(ballot.node.0.to_be_bytes());
    bytes
}

/// The transport of one process: it sends into the walk's mailbox (through
/// `outbox`) and receives what the walk delivered. Each send is judged by
/// the flight rule (rule 1 of §J4.2) against the sender's disk: an
/// acceptor's `Accepted` leaves only once a vote at least as new is on its
/// disk, and a `Promise` only once a promise at least as high is. A send
/// that breaks it fails with the walk's oracle text, `flight`.
pub struct Wire {
    /// What the walk delivered to this process.
    pub inbox: Rc<RefCell<VecDeque<Envelope>>>,
    /// What this process sent, for the walk to put in flight.
    pub outbox: Rc<RefCell<Vec<Envelope>>>,
    /// The disk of the process.
    pub disk: Disk,
    /// The text of the walk's flight oracle.
    pub flight: &'static str,
}

impl Transport for Wire {
    fn send(&mut self, envelope: Envelope) {
        let flight = self.flight;
        match &envelope.message {
            Message::Accepted { ballot, slot } => {
                let vote = self
                    .disk
                    .record(Key::new(envelope.sender(), Kind::Vote, slot.0));
                assert!(
                    vote.is_some_and(|v| v[..16] >= ballot_bytes(*ballot)[..]),
                    "{flight}: {envelope:?} leaves with no vote on the disk"
                );
            }
            Message::Promise { ballot, .. } => {
                let promise = self
                    .disk
                    .record(Key::new(envelope.sender(), Kind::Promise, 0));
                assert!(
                    promise.is_some_and(|p| p >= ballot_bytes(*ballot)),
                    "{flight}: {envelope:?} leaves with no promise on the disk"
                );
            }
            _ => {}
        }
        self.outbox.borrow_mut().push(envelope);
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.inbox.borrow_mut().pop_front()
    }
}
