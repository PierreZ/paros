//! The **on-disk format** of the journal roles (§J4.2, rule 4: the core
//! defines the ops and owns the format). A backend stores keys and bytes
//! and never reads them: every key and every value is built here.
//!
//! **The key.** A [`Key`] names one record of one role: its journal, its
//! node, its segment (a replica's; 0 for an acceptor), the kind of the
//! record and an index (a slot, or 0). Its bytes
//! ([`Key::to_bytes`]) sort as the key does, so a backend that sorts bytes
//! (a B-tree, an LSM tree, a sorted file) gives each role's records as one
//! range, in slot order. Each op is so namespaced by (journal, role).
//!
//! **The records of a role.**
//!
//! | Kind | Index | Value | Role |
//! |---|---|---|---|
//! | `Durable` | 0 | the highest [`PersistId`] of the role on the disk | every role with a disk |
//! | `Promise` | 0 | the promised ballot | acceptor |
//! | `Vote` | the slot | the ballot, then the command (the codec of `paros-journal`) | acceptor |
//! | `Base` | 0 | the first slot of the segment, then the boundary state | replica |
//! | `Chosen` | the slot | the command | replica |
//!
//! Each persist batch of a role ends with its `Durable` record, and a
//! batch is atomic (rule 4), so the role's highest durable id is on the
//! disk with the batch that made it durable.
//!
//! **Damage.** A record that the backend reports corrupt ([`Entry::Corrupt`])
//! or that does not decode is damage: [`decode_acceptor`] and
//! [`decode_replica`] return every damaged key, and the role does not start
//! (CTRL repair of a damaged record is a later rule group of #1326).

use std::collections::BTreeMap;

use paros_consensus::journal::JournalId;
use paros_journal::codec::{decode_boundary, decode_command, encode_boundary, encode_command};
use paros_journal::register::Ballot;
use paros_journal::roles::{AcceptorDisk, AcceptorOp, PersistId, ReplicaDisk, ReplicaOp};
use paros_journal::{NodeId, SegmentId, Slot};

use crate::wire::Address;

/// The kind of a record. The numbers are on the disk: never change one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// The highest durable batch of the role.
    Durable = 0,
    /// The acceptor's promise.
    Promise = 1,
    /// A vote of the acceptor.
    Vote = 2,
    /// The base of a replica's fold.
    Base = 3,
    /// A chosen slot that a replica holds.
    Chosen = 4,
}

impl Kind {
    const ALL: [Self; 5] = [
        Self::Durable,
        Self::Promise,
        Self::Vote,
        Self::Base,
        Self::Chosen,
    ];

    fn of(byte: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| *kind as u8 == byte)
    }
}

/// The key of one record of one role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    /// The role.
    pub address: Address,
    /// The kind of the record.
    pub kind: Kind,
    /// The slot of a `Vote` or a `Chosen` record; 0 for the others.
    pub index: u64,
}

/// The bytes that name a role: the journal, the node, the segment.
pub const PREFIX_LEN: usize = 8 + 8 + 8;

/// The bytes of a [`Key`]: the role, the kind, the index.
pub const KEY_LEN: usize = PREFIX_LEN + 1 + 8;

impl Key {
    /// The record `kind` at `index` of the role at `address`.
    #[must_use]
    pub fn new(address: Address, kind: Kind, index: u64) -> Self {
        Self {
            address,
            kind,
            index,
        }
    }

    /// The bytes of the key, big-endian, so that they sort as the key does.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; KEY_LEN] {
        let mut bytes = [0; KEY_LEN];
        bytes[..PREFIX_LEN].copy_from_slice(&Self::prefix(self.address));
        bytes[PREFIX_LEN] = self.kind as u8;
        bytes[PREFIX_LEN + 1..].copy_from_slice(&self.index.to_be_bytes());
        bytes
    }

    /// The key of `bytes`, or `None` when they are not a key.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != KEY_LEN {
            return None;
        }
        let journal = JournalId(u64::from_be_bytes(bytes[..8].try_into().ok()?));
        let node = NodeId(u64::from_be_bytes(bytes[8..16].try_into().ok()?));
        let segment = u64::from_be_bytes(bytes[16..PREFIX_LEN].try_into().ok()?);
        let kind = Kind::of(bytes[PREFIX_LEN])?;
        let index = u64::from_be_bytes(bytes[PREFIX_LEN + 1..].try_into().ok()?);
        let address = match segment {
            0 => Address::new(journal, node),
            segment => Address::replica(journal, node, SegmentId(segment)),
        };
        Some(Self::new(address, kind, index))
    }

    /// The bytes that every key of the role at `address` starts with.
    #[must_use]
    pub fn prefix(address: Address) -> [u8; PREFIX_LEN] {
        let mut bytes = [0; PREFIX_LEN];
        bytes[..8].copy_from_slice(&address.journal.0.to_be_bytes());
        bytes[8..16].copy_from_slice(&address.node.0.to_be_bytes());
        bytes[16..].copy_from_slice(&address.segment.map_or(0, |s| s.0).to_be_bytes());
        bytes
    }
}

/// One write of a storage commit: `value` at `key`. A later op of a commit
/// at the same key wins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    /// The key.
    pub key: Key,
    /// The bytes.
    pub value: Vec<u8>,
}

/// One record as a range read gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// The bytes that the record holds.
    Value(Vec<u8>),
    /// The backend found the record corrupt (a checksum that fails): the
    /// bytes are lost, the key is not.
    Corrupt,
}

/// The ops of one acceptor batch `id` of the role at `address`.
#[must_use]
pub fn acceptor_ops(address: Address, id: PersistId, ops: &[AcceptorOp]) -> Vec<Op> {
    let mut out: Vec<Op> = ops
        .iter()
        .map(|op| match op {
            AcceptorOp::SetPromise(ballot) => Op {
                key: Key::new(address, Kind::Promise, 0),
                value: ballot_bytes(*ballot).to_vec(),
            },
            AcceptorOp::AppendAccepted {
                slot,
                ballot,
                value,
            } => {
                let mut bytes = ballot_bytes(*ballot).to_vec();
                bytes.extend(encode_command(value));
                Op {
                    key: Key::new(address, Kind::Vote, slot.0),
                    value: bytes,
                }
            }
        })
        .collect();
    out.push(durable_op(address, id));
    out
}

/// The ops of one replica batch `id` of the role at `address`.
#[must_use]
pub fn replica_ops(address: Address, id: PersistId, ops: &[ReplicaOp]) -> Vec<Op> {
    let mut out: Vec<Op> = ops
        .iter()
        .map(|op| match op {
            ReplicaOp::Base { boundary, start } => {
                let mut bytes = start.0.to_be_bytes().to_vec();
                bytes.extend(encode_boundary(boundary));
                Op {
                    key: Key::new(address, Kind::Base, 0),
                    value: bytes,
                }
            }
            ReplicaOp::Chosen { slot, command } => Op {
                key: Key::new(address, Kind::Chosen, slot.0),
                value: encode_command(command),
            },
        })
        .collect();
    out.push(durable_op(address, id));
    out
}

/// The highest durable batch that a role's records name: the value of its
/// `Durable` record, if it reads.
#[must_use]
pub fn durable_of(entries: &[(Key, Entry)]) -> Option<PersistId> {
    entries
        .iter()
        .find_map(|(key, entry)| match (key.kind, entry) {
            (Kind::Durable, Entry::Value(bytes)) => u64_of(bytes).map(PersistId),
            _ => None,
        })
}

fn durable_op(address: Address, id: PersistId) -> Op {
    Op {
        key: Key::new(address, Kind::Durable, 0),
        value: id.0.to_be_bytes().to_vec(),
    }
}

/// The records of a role that are damaged: corrupt on the disk, not
/// readable as their kind, or of a kind that the role does not write.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Damage {
    /// The damaged keys, in key order.
    pub keys: Vec<Key>,
}

impl Damage {
    /// Whether no record is damaged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// The disk of the acceptor at `address`, from its records, or the damage
/// that stops it.
///
/// # Errors
///
/// The damaged records, when a record is corrupt, does not decode, is not
/// a record of an acceptor, or is not the acceptor's.
pub fn decode_acceptor(address: Address, entries: &[(Key, Entry)]) -> Result<AcceptorDisk, Damage> {
    let mut disk = AcceptorDisk::default();
    let mut damage = Damage::default();
    for (key, entry) in entries {
        let read = match (key.address == address, key.kind, entry) {
            (true, Kind::Durable, Entry::Value(bytes)) => {
                u64_of(bytes).map(|id| disk.durable = Some(PersistId(id)))
            }
            (true, Kind::Promise, Entry::Value(bytes)) => {
                ballot_of(bytes).map(|ballot| disk.promised = ballot)
            }
            (true, Kind::Vote, Entry::Value(bytes)) => bytes
                .split_at_checked(16)
                .and_then(|(ballot, command)| {
                    Some((ballot_of(ballot)?, decode_command(command).ok()?))
                })
                .map(|vote| {
                    disk.votes.insert(Slot(key.index), vote);
                }),
            _ => None,
        };
        if read.is_none() {
            damage.keys.push(*key);
        }
    }
    if damage.is_empty() {
        Ok(disk)
    } else {
        Err(damage)
    }
}

/// The disk of the replica at `address`, from its records, or the damage
/// that stops it.
///
/// # Errors
///
/// The damaged records, when a record is corrupt, does not decode, is not
/// a record of a replica, or is not the replica's.
pub fn decode_replica(address: Address, entries: &[(Key, Entry)]) -> Result<ReplicaDisk, Damage> {
    let mut durable = None;
    let mut base = None;
    let mut chosen = BTreeMap::new();
    let mut damage = Damage::default();
    for (key, entry) in entries {
        let read = match (key.address == address, key.kind, entry) {
            (true, Kind::Durable, Entry::Value(bytes)) => {
                u64_of(bytes).map(|id| durable = Some(PersistId(id)))
            }
            (true, Kind::Base, Entry::Value(bytes)) => bytes
                .split_at_checked(8)
                .and_then(|(start, boundary)| {
                    Some((decode_boundary(boundary).ok()?, Slot(u64_of(start)?)))
                })
                .map(|read| base = Some(read)),
            (true, Kind::Chosen, Entry::Value(bytes)) => decode_command(bytes).ok().map(|c| {
                chosen.insert(Slot(key.index), c);
            }),
            _ => None,
        };
        if read.is_none() {
            damage.keys.push(*key);
        }
    }
    if damage.is_empty() {
        Ok(ReplicaDisk {
            base,
            chosen,
            durable,
        })
    } else {
        Err(damage)
    }
}

fn ballot_bytes(ballot: Ballot) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&ballot.round.to_be_bytes());
    bytes[8..].copy_from_slice(&ballot.node.0.to_be_bytes());
    bytes
}

fn ballot_of(bytes: &[u8]) -> Option<Ballot> {
    let (round, node) = bytes.split_at_checked(8)?;
    Some(Ballot {
        round: u64_of(round)?,
        node: NodeId(u64_of(node)?),
    })
}

fn u64_of(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

#[cfg(test)]
mod tests;
