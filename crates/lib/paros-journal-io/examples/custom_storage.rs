//! **A storage backend of your own, checked by the contract suite.**
//!
//! Run it: `cargo run -p paros-journal-io --example custom_storage`
//!
//! A program brings its own disk by implementing one trait,
//! [`Storage`]: an atomic `commit` of ops and a `scan` of one role's
//! records. The bytes are the journal's (`paros_journal_io::layout`); the
//! backend only stores them.
//!
//! [`LogStorage`] here is such a backend, written by hand: an append-only
//! log of batches, as a file would hold it. Each batch is framed with its
//! length and a checksum, so a batch that a crash cut in the middle is
//! found at the next open and dropped whole: the batch is atomic, as the
//! trait asks. The "file" is a `Vec<u8>`; swap it for a real file and
//! nothing else changes.
//!
//! # What the run shows
//!
//! 1. The contract suite of the storage trait passes on `LogStorage`.
//! 2. A journal runs on it in one process: writer A writes three records.
//! 3. The process crashes after a commit that the crash cut in the middle.
//!    The log is opened again: the cut batch is gone whole, every batch
//!    before it is there, and the roles start from it.
//!
//! Each step asserts its result, so CI runs this example as a test.

mod common;

use std::collections::BTreeMap;

use paros_journal::Slot;
use paros_journal_io::contract;
use paros_journal_io::layout::KEY_LEN;
use paros_journal_io::mem::{ManualClock, MemNetwork, MemRegister};
use paros_journal_io::{
    Address, Driver, Entry, HostConfig, Journal, Key, Op, Storage, StorageError,
};

use common::{
    JOURNAL, TICK, acceptor, at, claim, elected, leader, open, replica, reply_of, reshape, show,
    write, written,
};

/// An append-only log of batches. Each batch on the log is: its length
/// (4 bytes), its checksum (8 bytes), then each op as a key (25 bytes),
/// a value length (4 bytes) and the value.
#[derive(Clone, Default)]
struct LogStorage {
    /// The bytes of the log file.
    file: Vec<u8>,
    /// The records, rebuilt from the log at open, then kept up to date.
    index: BTreeMap<[u8; KEY_LEN], Vec<u8>>,
}

/// A checksum of `bytes` (FNV-1a): enough to find a batch cut short.
fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

impl LogStorage {
    /// Open the log in `file`: replay every whole batch, and drop the tail
    /// from the first batch that is cut or does not match its checksum.
    fn open(file: &[u8]) -> Self {
        let mut storage = Self::default();
        let mut at = 0;
        while let Some(batch) = Self::batch_at(file, at) {
            for (key, value) in Self::ops_of(batch) {
                storage.index.insert(key, value);
            }
            at += 12 + batch.len();
        }
        storage.file = file[..at].to_vec();
        storage
    }

    fn batch_at(file: &[u8], at: usize) -> Option<&[u8]> {
        let len = u32::from_be_bytes(file.get(at..at + 4)?.try_into().ok()?) as usize;
        let sum = u64::from_be_bytes(file.get(at + 4..at + 12)?.try_into().ok()?);
        let batch = file.get(at + 12..at + 12 + len)?;
        (checksum(batch) == sum).then_some(batch)
    }

    fn ops_of(mut batch: &[u8]) -> Vec<([u8; KEY_LEN], Vec<u8>)> {
        let mut ops = Vec::new();
        while !batch.is_empty() {
            let key: [u8; KEY_LEN] = batch[..KEY_LEN].try_into().expect("a key");
            let len = u32::from_be_bytes(batch[KEY_LEN..KEY_LEN + 4].try_into().expect("4 bytes"));
            let end = KEY_LEN + 4 + len as usize;
            ops.push((key, batch[KEY_LEN + 4..end].to_vec()));
            batch = &batch[end..];
        }
        ops
    }
}

impl Storage<Journal> for LogStorage {
    fn commit(&mut self, ops: &[Op]) -> Result<(), StorageError> {
        let mut batch = Vec::new();
        for op in ops {
            batch.extend(op.key.to_bytes());
            let len = u32::try_from(op.value.len())
                .map_err(|_| StorageError("a value above 4 GiB".into()))?;
            batch.extend(len.to_be_bytes());
            batch.extend(&op.value);
        }
        let len =
            u32::try_from(batch.len()).map_err(|_| StorageError("a batch above 4 GiB".into()))?;
        // One append; a real file would `write_all` then `sync_data` here.
        self.file.extend(len.to_be_bytes());
        self.file.extend(checksum(&batch).to_be_bytes());
        self.file.extend(&batch);
        for op in ops {
            self.index.insert(op.key.to_bytes(), op.value.clone());
        }
        Ok(())
    }

    fn scan(&mut self, address: Address) -> Result<Vec<(Key, Entry)>, StorageError> {
        let prefix = Key::prefix(address);
        Ok(self
            .index
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(key, value)| {
                let key = Key::from_bytes(key).expect("the log holds keys only");
                (key, Entry::Value(value.clone()))
            })
            .collect())
    }
}

type Process = Driver<LogStorage, paros_journal_io::mem::Endpoint, ManualClock, MemRegister, ()>;

const CONFIG: HostConfig = HostConfig {
    tick: TICK,
    resend_page: 16,
    window: 64,
};
const NODES: [u64; 6] = [1, 2, 3, 11, 12, 13];
const L1: u64 = 100;

fn main() {
    println!("1. The contract suite of the storage trait.");
    contract::storage(LogStorage::default);
    contract::storage_reopen(LogStorage::default, |log| LogStorage::open(&log.file));
    show("contract::storage on LogStorage", &"passed");

    println!("2. A journal on LogStorage writes three records.");
    let network = MemNetwork::new();
    let clock = ManualClock::new();
    let register = MemRegister::new();
    register.create(JOURNAL, elected());
    let mut process = start(&network, &clock, &register, LogStorage::default(), true);
    let run = |process: &mut Process, call| {
        for _ in 0..100 {
            while process.poll() {}
            if let Some(outcome) = reply_of(&process.take_replies(), call) {
                return outcome.clone();
            }
            clock.advance(TICK);
        }
        panic!("call {call}: no answer in 100 ticks");
    };
    for _ in 0..100 {
        while process.poll() {}
        if open(&process, L1) {
            break;
        }
        clock.advance(TICK);
    }
    let call = process.call(at(L1), claim());
    show("SetLeader(A, Unset)", &run(&mut process, call));
    for seq in 0..3 {
        let call = process.call(at(L1), write(seq, &format!("a{seq}")));
        let answer = run(&mut process, call);
        show(&format!("Write(A, {seq})"), &answer);
        assert_eq!(answer, written(seq));
    }

    println!("3. A crash cuts the last batch; the log is opened again.");
    let log = process.into_parts().0;
    let whole = log.file.len();
    let cut = LogStorage::open(&log.file[..whole - 3]);
    show(
        "bytes of the log, then after the open",
        &(whole, cut.file.len()),
    );
    assert!(cut.file.len() < whole - 3, "the cut batch is dropped whole");
    let last = LogStorage::batch_at(&log.file, cut.file.len()).expect("the last whole batch");
    assert_eq!(
        cut.file.len() + 12 + last.len(),
        whole,
        "only the last batch is cut"
    );
    assert!(
        LogStorage::ops_of(last)
            .iter()
            .any(|(key, value)| cut.index.get(key) != Some(value)),
        "the ops of the cut batch are not in the log"
    );
    let process = start(&network, &clock, &register, cut, false);
    let started = NODES
        .iter()
        .filter(|n| {
            let host = process.host();
            host.acceptor(at(**n)).is_some() || host.replica(at(**n)).is_some()
        })
        .count();
    show("roles that start from the log", &started);
    assert_eq!(started, NODES.len(), "every role reads its records back");
    println!("ok: the contract holds, and a cut batch is dropped whole.");
}

/// A process on `storage` that serves the acceptors and the replicas of
/// segment 1, and leader L1 when `lead`.
fn start(
    network: &MemNetwork,
    clock: &ManualClock,
    register: &MemRegister,
    storage: LogStorage,
    lead: bool,
) -> Process {
    use paros_journal_io::Register as _;
    let mut register = register.clone();
    if lead {
        let value = register.read(JOURNAL).expect("the register");
        register
            .change(JOURNAL, &reshape(&value, 1))
            .expect("segment 1 opens");
    }
    let mut process = Driver::new(
        CONFIG,
        storage,
        network.endpoint(),
        clock.clone(),
        register,
        (),
    );
    let mut roles: Vec<_> = NODES[..3].iter().map(|n| acceptor(*n)).collect();
    roles.extend(NODES[3..].iter().map(|n| replica(*n, 1)));
    if lead {
        roles.push(leader(L1, 1, Slot(0)));
    }
    for role in roles {
        process.transport_mut().route(role.address());
        process.assign(role);
    }
    process
}
