//! The **codec** of the store: the bytes of an [`Entry`] in one journal
//! [`Record`].
//!
//! An entry of the store is a record with one client header, key
//! [`HEADER_KEY`], whose value is the format number ([`FORMAT`], one
//! byte); the payload is the entry. A record with no such header, with
//! another format, or with a payload that does not decode is not an entry:
//! the fold voids it ([`crate::tree::Fate::Foreign`]). Every member decodes
//! the same bytes the same way, so the fold stays the same on every member.
//! **Open**: the name of the key.
//!
//! The format, all integers little endian:
//!
//! - an entry: a tag, `1` `Open` (session, transaction), `2` `Txn`
//!   (session, `op_seq`, transaction), `3` `Expire` (session, `last`), `4`
//!   `Checkpoint` (`at`, state); then nothing.
//! - a transaction: the list of checks, then the list of operations. A
//!   check: `1` version (path, `u64`), `2` holds (path, term). An
//!   operation: `1` create (path, data, ephemeral, sequential), `2` ensure
//!   (path), `3` set data (path, data, optional version), `4` delete (path,
//!   optional version).
//! - a state: `next`, the nodes (path, data, version, created, modified,
//!   optional owner), the sessions (id, opened, last, `op_seq`), the
//!   tombstones, the turns (parent, holder, term), the checkpoints.
//! - a byte string or a path: `len: u32`, then the bytes. A list:
//!   `count: u32`, then the items. An option: `0`, or `1` then the value.
//!   A flag: `0` or `1`. A session id, a position and a version: `u64`.
//!
//! A decoder never trusts a length: it reads only the bytes that are
//! there, and refuses a path that breaks a rule of [`Path`].

use std::collections::{BTreeMap, BTreeSet};

use paros_journal::{Header, Record, Seq};

use crate::entry::{Check, Entry, Op, SessionId, Term, Txn};
use crate::path::Path;
use crate::tree::{Node, Row, Tree, Turn};

/// The key of the client header that marks a record as an entry of the
/// store.
pub const HEADER_KEY: &[u8] = b"paros-election";

/// The format this code writes, and the only one it reads.
pub const FORMAT: u8 = 1;

/// The record that holds `entry`.
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn encode(entry: &Entry) -> Record {
    let mut out = Vec::new();
    put_entry(&mut out, entry);
    let mut record = Record::new(out);
    record.headers.client.push(Header {
        key: HEADER_KEY.to_vec(),
        value: vec![FORMAT],
    });
    assert!(
        decode(&record).as_ref() == Some(entry),
        "an entry decodes as it was encoded"
    );
    record
}

/// The entry that `record` holds, or `None` when it is not an entry of the
/// store.
#[must_use]
pub fn decode(record: &Record) -> Option<Entry> {
    let mut marks = record.headers.client.iter().filter(|h| h.key == HEADER_KEY);
    let mark = marks.next()?;
    if marks.next().is_some() || mark.value != [FORMAT] {
        return None;
    }
    let mut reader = Reader {
        bytes: &record.payload,
    };
    let entry = reader.entry()?;
    reader.bytes.is_empty().then_some(entry)
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("a length of the store fits in u32");
    out.extend_from_slice(&len.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn put_path(out: &mut Vec<u8>, path: &Path) {
    put_bytes(out, path.as_str().as_bytes());
}

fn put_option(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            put_u64(out, v);
        }
    }
}

fn put_txn(out: &mut Vec<u8>, txn: &Txn) {
    put_len(out, txn.checks.len());
    for check in &txn.checks {
        match check {
            Check::Version { path, version } => {
                out.push(1);
                put_path(out, path);
                put_u64(out, *version);
            }
            Check::Holds { parent, term } => {
                out.push(2);
                put_path(out, parent);
                put_u64(out, term.0.0);
            }
        }
    }
    put_len(out, txn.ops.len());
    for op in &txn.ops {
        match op {
            Op::Create {
                path,
                data,
                ephemeral,
                sequential,
            } => {
                out.push(1);
                put_path(out, path);
                put_bytes(out, data);
                out.push(u8::from(*ephemeral));
                out.push(u8::from(*sequential));
            }
            Op::Ensure { path } => {
                out.push(2);
                put_path(out, path);
            }
            Op::SetData {
                path,
                data,
                version,
            } => {
                out.push(3);
                put_path(out, path);
                put_bytes(out, data);
                put_option(out, *version);
            }
            Op::Delete { path, version } => {
                out.push(4);
                put_path(out, path);
                put_option(out, *version);
            }
        }
    }
}

fn put_tree(out: &mut Vec<u8>, tree: &Tree) {
    put_u64(out, tree.next.0);
    put_len(out, tree.nodes.len());
    for (path, node) in &tree.nodes {
        put_path(out, path);
        put_bytes(out, &node.data);
        put_u64(out, node.version);
        put_u64(out, node.created.0);
        put_u64(out, node.modified.0);
        put_option(out, node.owner.map(|s| s.0));
    }
    put_len(out, tree.sessions.len());
    for (session, row) in &tree.sessions {
        put_u64(out, session.0);
        put_u64(out, row.opened.0);
        put_u64(out, row.last.0);
        put_u64(out, row.op_seq);
    }
    put_len(out, tree.tombs.len());
    for session in &tree.tombs {
        put_u64(out, session.0);
    }
    put_len(out, tree.turns.len());
    for (parent, turn) in &tree.turns {
        put_path(out, parent);
        put_u64(out, turn.holder.0);
        put_u64(out, turn.term.0.0);
    }
    put_len(out, tree.checkpoints.len());
    for seq in &tree.checkpoints {
        put_u64(out, seq.0);
    }
}

fn put_entry(out: &mut Vec<u8>, entry: &Entry) {
    match entry {
        Entry::Open { session, txn } => {
            out.push(1);
            put_u64(out, session.0);
            put_txn(out, txn);
        }
        Entry::Txn {
            session,
            op_seq,
            txn,
        } => {
            out.push(2);
            put_u64(out, session.0);
            put_u64(out, *op_seq);
            put_txn(out, txn);
        }
        Entry::Expire { session, last } => {
            out.push(3);
            put_u64(out, session.0);
            put_u64(out, last.0);
        }
        Entry::Checkpoint { at, tree } => {
            out.push(4);
            put_u64(out, at.0);
            put_tree(out, tree);
        }
    }
}

/// The bytes a decoder has not read yet.
struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.bytes.len() < n {
            return None;
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u64(&mut self) -> Option<u64> {
        let bytes = self.take(8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn seq(&mut self) -> Option<Seq> {
        self.u64().map(Seq)
    }

    fn flag(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    /// A count of items, each at least one byte: never more than the bytes
    /// left, so a forged count allocates nothing.
    fn len(&mut self) -> Option<usize> {
        let bytes = self.take(4)?;
        let len = usize::try_from(u32::from_le_bytes(bytes.try_into().ok()?)).ok()?;
        (len <= self.bytes.len()).then_some(len)
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.len()?;
        self.take(len).map(<[u8]>::to_vec)
    }

    fn path(&mut self) -> Option<Path> {
        let bytes = self.bytes()?;
        let text = std::str::from_utf8(&bytes).ok()?;
        Path::new(text).ok()
    }

    /// An optional `u64`: `Ok(None)`, `Ok(Some(v))`, or `Err(())` when the
    /// bytes are not one.
    fn option(&mut self) -> Result<Option<u64>, ()> {
        match self.u8().ok_or(())? {
            0 => Ok(None),
            1 => self.u64().map(Some).ok_or(()),
            _ => Err(()),
        }
    }

    fn txn(&mut self) -> Option<Txn> {
        let count = self.len()?;
        let mut checks = Vec::with_capacity(count);
        for _ in 0..count {
            checks.push(match self.u8()? {
                1 => Check::Version {
                    path: self.path()?,
                    version: self.u64()?,
                },
                2 => Check::Holds {
                    parent: self.path()?,
                    term: Term(self.seq()?),
                },
                _ => return None,
            });
        }
        let count = self.len()?;
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            ops.push(match self.u8()? {
                1 => Op::Create {
                    path: self.path()?,
                    data: self.bytes()?,
                    ephemeral: self.flag()?,
                    sequential: self.flag()?,
                },
                2 => Op::Ensure { path: self.path()? },
                3 => Op::SetData {
                    path: self.path()?,
                    data: self.bytes()?,
                    version: self.option().ok()?,
                },
                4 => Op::Delete {
                    path: self.path()?,
                    version: self.option().ok()?,
                },
                _ => return None,
            });
        }
        Some(Txn { checks, ops })
    }

    fn tree(&mut self) -> Option<Tree> {
        let next = self.seq()?;
        let mut nodes = BTreeMap::new();
        for _ in 0..self.len()? {
            let path = self.path()?;
            let node = Node {
                data: self.bytes()?,
                version: self.u64()?,
                created: self.seq()?,
                modified: self.seq()?,
                owner: self.option().ok()?.map(SessionId),
            };
            nodes.insert(path, node);
        }
        let mut sessions = BTreeMap::new();
        for _ in 0..self.len()? {
            let session = SessionId(self.u64()?);
            let row = Row {
                opened: self.seq()?,
                last: self.seq()?,
                op_seq: self.u64()?,
            };
            sessions.insert(session, row);
        }
        let mut tombs = BTreeSet::new();
        for _ in 0..self.len()? {
            tombs.insert(SessionId(self.u64()?));
        }
        let mut turns = BTreeMap::new();
        for _ in 0..self.len()? {
            let parent = self.path()?;
            let turn = Turn {
                holder: SessionId(self.u64()?),
                term: Term(self.seq()?),
            };
            turns.insert(parent, turn);
        }
        let mut checkpoints = BTreeSet::new();
        for _ in 0..self.len()? {
            checkpoints.insert(self.seq()?);
        }
        Some(Tree {
            next,
            nodes,
            sessions,
            tombs,
            turns,
            checkpoints,
        })
    }

    fn entry(&mut self) -> Option<Entry> {
        Some(match self.u8()? {
            1 => Entry::Open {
                session: SessionId(self.u64()?),
                txn: self.txn()?,
            },
            2 => Entry::Txn {
                session: SessionId(self.u64()?),
                op_seq: self.u64()?,
                txn: self.txn()?,
            },
            3 => Entry::Expire {
                session: SessionId(self.u64()?),
                last: self.seq()?,
            },
            4 => Entry::Checkpoint {
                at: self.seq()?,
                tree: Box::new(self.tree()?),
            },
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests;
