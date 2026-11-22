//! Each suite fails on a backend that breaks its contract.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use super::*;
use crate::mem::MemStorage;
use crate::storage::StorageError;
use crate::wire::Time;

/// A disk that writes and reads nothing.
struct Forgetful;

impl Storage for Forgetful {
    fn commit(&mut self, _ops: &[Op], _sync: bool) -> Result<(), StorageError> {
        Ok(())
    }

    fn scan(&mut self, _address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        Ok(Vec::new())
    }
}

#[test]
#[should_panic(expected = "storage: a scan gives the role's records of a commit")]
fn the_storage_suite_fails_a_disk_that_forgets() {
    storage(|| Forgetful);
}

#[test]
#[should_panic(expected = "storage: a synced commit is seen after a reopen")]
fn the_reopen_suite_fails_a_disk_that_loses_a_synced_commit() {
    storage_reopen(MemStorage::new, |_| MemStorage::new());
}

/// A network that changes the sender of each message. Its two ends share
/// one inbox.
#[derive(Clone, Default)]
struct Forger {
    inbox: Rc<RefCell<VecDeque<Envelope>>>,
}

impl Transport for Forger {
    fn send(&mut self, mut envelope: Envelope) {
        envelope.from = NodeId(u64::MAX);
        self.inbox.borrow_mut().push_back(envelope);
    }

    fn receive(&mut self) -> Option<Envelope> {
        self.inbox.borrow_mut().pop_front()
    }
}

#[test]
#[should_panic(expected = "transport: a message arrives as it was sent")]
fn the_transport_suite_fails_a_network_that_changes_a_message() {
    let mut sender = Forger::default();
    let mut receiver = sender.clone();
    transport(
        &mut sender,
        &mut receiver,
        Address::new(JournalId(1), NodeId(1)),
        || {},
    );
}

/// A clock that goes back.
struct Back(std::cell::Cell<u64>);

impl Clock for Back {
    fn now(&self) -> Time {
        let now = self.0.get();
        self.0.set(now.saturating_sub(1));
        Time(now)
    }
}

#[test]
#[should_panic(expected = "clock: the time never goes back")]
fn the_clock_suite_fails_a_clock_that_goes_back() {
    clock(&Back(std::cell::Cell::new(10)), || {});
}
