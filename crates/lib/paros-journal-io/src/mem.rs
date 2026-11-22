//! Backends in memory, written with the standard library alone: a storage,
//! a network of processes, a clock that a test moves, and a register.
//!
//! They serve the tests, the examples and a program that runs a whole
//! journal in one process. Each one keeps the contract of its trait, and
//! [`crate::contract`] checks it. The handles that share a state (the
//! endpoints of one network, the copies of one clock or one register) use
//! `Rc`: nothing here is `Send`, and nothing needs to be.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use paros_consensus::journal::JournalId;
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};

use crate::clock::Clock;
use crate::layout::{Entry, KEY_LEN, Key, Op, PREFIX_LEN};
use crate::register::Register;
use crate::storage::{Storage, StorageError};
use crate::transport::Transport;
use crate::wire::{Address, Envelope, Time};

/// A storage in memory: sorted keys to bytes. Every commit is durable at
/// once. A test can mark a record corrupt, make the next commit fail, or
/// lose records.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    records: BTreeMap<[u8; KEY_LEN], Vec<u8>>,
    corrupt: BTreeSet<[u8; KEY_LEN]>,
    fail_next: bool,
}

impl MemStorage {
    /// An empty storage.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the storage holds no record.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// From now on, a scan reports the record at `key` corrupt.
    pub fn corrupt(&mut self, key: Key) {
        self.corrupt.insert(key.to_bytes());
    }

    /// The next commit fails, and writes nothing.
    pub fn fail_next_commit(&mut self) {
        self.fail_next = true;
    }

    /// Lose every record whose key `lost` names: a bug that a test plants,
    /// never a fault of the contract (a commit that was durable stays).
    ///
    /// # Panics
    ///
    /// When the storage holds bytes that are not a key: a commit stores
    /// keys only.
    pub fn lose(&mut self, mut lost: impl FnMut(&Key) -> bool) {
        self.records.retain(|bytes, _| {
            !lost(&Key::from_bytes(bytes).expect("the storage holds keys only"))
        });
    }
}

impl Storage for MemStorage {
    fn commit(&mut self, ops: &[Op], _sync: bool) -> Result<(), StorageError> {
        if std::mem::take(&mut self.fail_next) {
            return Err(StorageError("a planted failure".into()));
        }
        for op in ops {
            let key = op.key.to_bytes();
            self.corrupt.remove(&key);
            self.records.insert(key, op.value.clone());
        }
        Ok(())
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        let prefix = Key::prefix(address);
        Ok(self
            .records
            .range(prefix_start(prefix)..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(bytes, value)| {
                let key = Key::from_bytes(bytes).expect("the storage holds keys only");
                let entry = if self.corrupt.contains(bytes) {
                    Entry::Corrupt
                } else {
                    Entry::Value(value.clone())
                };
                (key, entry)
            })
            .collect())
    }
}

fn prefix_start(prefix: [u8; PREFIX_LEN]) -> [u8; KEY_LEN] {
    let mut start = [0; KEY_LEN];
    start[..PREFIX_LEN].copy_from_slice(&prefix);
    start
}

/// The messages of a network in memory, by endpoint.
#[derive(Debug, Default)]
struct Network {
    /// The endpoint that serves each node.
    routes: BTreeMap<Address, usize>,
    inboxes: Vec<VecDeque<Envelope>>,
}

/// A network in memory that links many processes: each process has an
/// [`Endpoint`]. A message to a role that no endpoint serves is lost.
/// Delivery is in order.
#[derive(Clone, Debug, Default)]
pub struct MemNetwork {
    network: Rc<RefCell<Network>>,
}

impl MemNetwork {
    /// A network with no endpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A new endpoint, for one process.
    #[must_use]
    pub fn endpoint(&self) -> Endpoint {
        let mut network = self.network.borrow_mut();
        network.inboxes.push(VecDeque::new());
        Endpoint {
            network: Rc::clone(&self.network),
            id: network.inboxes.len() - 1,
        }
    }

    /// The messages that wait in every inbox.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.network
            .borrow()
            .inboxes
            .iter()
            .map(VecDeque::len)
            .sum()
    }
}

/// The transport of one process on a [`MemNetwork`].
#[derive(Debug)]
pub struct Endpoint {
    network: Rc<RefCell<Network>>,
    id: usize,
}

impl Endpoint {
    /// From now on, the messages to the node of `address` come to this
    /// endpoint (every role of the node: each segment of a replica node).
    pub fn route(&self, address: Address) {
        self.network
            .borrow_mut()
            .routes
            .insert(address.node_address(), self.id);
    }
}

impl Transport for Endpoint {
    fn send(&mut self, envelope: Envelope) {
        let mut network = self.network.borrow_mut();
        if let Some(to) = network
            .routes
            .get(&envelope.receiver().node_address())
            .copied()
        {
            network.inboxes[to].push_back(envelope);
        }
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.network.borrow_mut().inboxes[self.id].pop_front()
    }
}

/// A clock that only a test moves. Its copies share the time.
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    now: Rc<Cell<u64>>,
}

impl ManualClock {
    /// A clock at time 0.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the time `nanos` nanoseconds forward.
    pub fn advance(&self, nanos: u64) {
        self.now.set(self.now.get().saturating_add(nanos));
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Time {
        Time(self.now.get())
    }
}

/// The registers of many journals, in memory: a change is done or refused
/// at once. Its copies share the registers.
#[derive(Clone, Debug, Default)]
pub struct MemRegister {
    values: Rc<RefCell<BTreeMap<JournalId, RegisterValue>>>,
}

impl MemRegister {
    /// No register yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The register of `journal`, at `value`.
    pub fn create(&self, journal: JournalId, value: RegisterValue) {
        self.values.borrow_mut().insert(journal, value);
    }

    /// The value of the register of `journal`.
    #[must_use]
    pub fn value(&self, journal: JournalId) -> Option<RegisterValue> {
        self.values.borrow().get(&journal).cloned()
    }
}

impl Register for MemRegister {
    fn change(
        &mut self,
        journal: JournalId,
        change: &Change,
    ) -> Result<RegisterValue, ChangeRefusal> {
        let mut values = self.values.borrow_mut();
        let value = values.entry(journal).or_default();
        let next = value.apply(change)?;
        *value = next.clone();
        Ok(next)
    }

    fn read(&mut self, journal: JournalId) -> Option<RegisterValue> {
        self.value(journal)
    }
}

#[cfg(test)]
mod tests;
