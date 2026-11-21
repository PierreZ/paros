//! The **codec** of the journal: the bytes of its commands, records,
//! segment records and boundary states, on the wire and on disk (the core
//! owns the on-disk format, `docs/journal/04-roles.md` §J4.2, rule 4).
//!
//! **Every encoding starts with a header**: the release number (`u16`) and
//! the kind of the value (`u8`). A decoder refuses a release it does not
//! know (the upgrade minimum of v1, §J1.1): v1 knows only [`RELEASE`]. A
//! segment record also carries the release it was written with, and the
//! decoder refuses that one too when it does not know it. The command
//! `Upgrade` is reserved for the move to a later release
//! ([`Command::Upgrade`]).
//!
//! The format, all integers little endian:
//!
//! - header: `release: u16`, `kind: u8` (1 command, 2 record, 3 segment
//!   record, 4 boundary state); then the value, then nothing.
//! - a byte string: `len: u32`, then the bytes.
//! - a list: `count: u32`, then the items.
//! - an option: `0` for none, or `1` then the value.
//! - a leader value: `0` unset, `1` multi-writer, `2` then a set `u128`.
//! - a command: a tag, `0` `Noop`, `1` `Write` (the fence: `0` open, or `1`
//!   then the uuid and `expected_seq`; then the list of records), `2`
//!   `Truncate` (an optional uuid, `up_to`), `3` `SetLeader` (an optional
//!   uuid `new`, the leader value `old`), `4` `Upgrade` (a release).
//! - a record (§J2.8): the paros headers, the client headers, the payload.
//!   The paros headers are the **reserved key space** of paros: a list
//!   (`count: u16`) of fields, each a key `u16` and a byte string. Key 1 is
//!   the leader time (8 bytes). A key that the release does not know is
//!   refused: a new paros header comes with a new release. The client
//!   headers are a list of pairs of byte strings, opaque.
//!
//! A decoder never trusts a length: it reads only the bytes that are there,
//! and a value that breaks a rule of its type (an unset uuid as a leader, a
//! grid that is not a v1 grid, a record whose fields disagree) is refused.

use paros_consensus::journal::{LeaderUuid, Seq};
use paros_consensus::{NodeId, Slot};

use crate::command::{Command, Fence, Header, Headers, Leader, Record, Write};
use crate::fold::BoundaryState;
use crate::segment::{Grid, OpeningCall, SegmentId, SegmentRecord, SegmentState};

/// A **release number** of the journal's formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Release(pub u16);

/// The release this code writes, and the only one it reads.
pub const RELEASE: Release = Release(1);

/// The kind of value an encoding holds.
const COMMAND: u8 = 1;
const RECORD: u8 = 2;
const SEGMENT: u8 = 3;
const BOUNDARY: u8 = 4;

/// The paros header key of the leader time.
const LEADER_TIME: u16 = 1;

/// The bytes of a header: a release and a kind.
const HEADER_LEN: usize = 3;
const _: () = assert!(HEADER_LEN == size_of::<u16>() + size_of::<u8>());
const _: () = assert!(RELEASE.0 > 0);

/// Why bytes are not a value of the journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The bytes end before the value does.
    Short,
    /// A release that this code does not know.
    UnknownRelease(Release),
    /// The header names another kind of value.
    Kind {
        /// The kind the decoder reads.
        expected: u8,
        /// The kind the header names.
        found: u8,
    },
    /// A tag that names no variant.
    Tag {
        /// What the tag selects.
        what: &'static str,
        /// The tag found.
        tag: u8,
    },
    /// A paros header key that this release does not know.
    ParosKey(u16),
    /// A value that breaks a rule of its type.
    Invalid(&'static str),
    /// Bytes are left after the value.
    Trailing(usize),
}

/// The bytes of `command`.
#[must_use]
pub fn encode_command(command: &Command) -> Vec<u8> {
    let mut out = Out::new(COMMAND);
    out.command(command);
    out.finish()
}

/// The command in `bytes`.
///
/// # Errors
///
/// When the bytes are not one command of a known release.
pub fn decode_command(bytes: &[u8]) -> Result<Command, DecodeError> {
    let mut input = In::new(bytes, COMMAND)?;
    let command = input.command()?;
    input.finish()?;
    Ok(command)
}

/// The bytes of `record`.
#[must_use]
pub fn encode_record(record: &Record) -> Vec<u8> {
    let mut out = Out::new(RECORD);
    out.record(record);
    out.finish()
}

/// The record in `bytes`.
///
/// # Errors
///
/// When the bytes are not one record of a known release.
pub fn decode_record(bytes: &[u8]) -> Result<Record, DecodeError> {
    let mut input = In::new(bytes, RECORD)?;
    let record = input.record()?;
    input.finish()?;
    Ok(record)
}

/// The bytes of `record`.
#[must_use]
pub fn encode_segment(record: &SegmentRecord) -> Vec<u8> {
    let mut out = Out::new(SEGMENT);
    out.segment(record);
    out.finish()
}

/// The segment record in `bytes`.
///
/// # Errors
///
/// When the bytes are not one segment record of a known release, or its
/// fields break a rule of the record.
pub fn decode_segment(bytes: &[u8]) -> Result<SegmentRecord, DecodeError> {
    let mut input = In::new(bytes, SEGMENT)?;
    let record = input.segment()?;
    input.finish()?;
    Ok(record)
}

/// The bytes of `boundary`.
#[must_use]
pub fn encode_boundary(boundary: &BoundaryState) -> Vec<u8> {
    let mut out = Out::new(BOUNDARY);
    out.boundary(boundary);
    out.finish()
}

/// The boundary state in `bytes`.
///
/// # Errors
///
/// When the bytes are not one boundary state of a known release.
pub fn decode_boundary(bytes: &[u8]) -> Result<BoundaryState, DecodeError> {
    let mut input = In::new(bytes, BOUNDARY)?;
    let boundary = input.boundary()?;
    input.finish()?;
    Ok(boundary)
}

/// An encoding in progress.
struct Out {
    bytes: Vec<u8>,
}

impl Out {
    fn new(kind: u8) -> Self {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&RELEASE.0.to_le_bytes());
        bytes.push(kind);
        assert!(
            bytes.len() == HEADER_LEN,
            "a header is a release and a kind"
        );
        assert!(kind != 0, "a kind is not 0");
        Self { bytes }
    }

    fn finish(self) -> Vec<u8> {
        assert!(self.bytes.len() > HEADER_LEN, "a value follows its header");
        self.bytes
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32_len(&mut self, len: usize) {
        let len = u32::try_from(len).expect("a length of the journal's formats fits a u32");
        self.bytes.extend_from_slice(&len.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u32_len(value.len());
        self.bytes.extend_from_slice(value);
    }

    fn option<T>(&mut self, value: Option<T>, mut some: impl FnMut(&mut Self, T)) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                some(self, value);
            }
        }
    }

    fn uuid(&mut self, uuid: LeaderUuid) {
        assert!(uuid.is_set(), "an encoded uuid is set");
        self.u128(uuid.0);
    }

    fn leader(&mut self, leader: Leader) {
        assert!(leader.is_valid(), "an encoded leader is valid");
        match leader {
            Leader::Unset => self.u8(0),
            Leader::Multi => self.u8(1),
            Leader::Uuid(uuid) => {
                self.u8(2);
                self.uuid(uuid);
            }
        }
    }

    fn command(&mut self, command: &Command) {
        match command {
            Command::Noop => self.u8(0),
            Command::Write(write) => {
                self.u8(1);
                match write.fence {
                    Fence::Open => self.u8(0),
                    Fence::Leader { uuid, expected_seq } => {
                        self.u8(1);
                        self.uuid(uuid);
                        self.u64(expected_seq.0);
                    }
                }
                self.u32_len(write.records.len());
                for record in &write.records {
                    self.record(record);
                }
            }
            Command::Truncate { fence, up_to } => {
                self.u8(2);
                self.option(*fence, Self::uuid);
                self.u64(up_to.0);
            }
            Command::SetLeader { new, old } => {
                self.u8(3);
                self.option(*new, Self::uuid);
                self.leader(*old);
            }
            Command::Upgrade { release } => {
                self.u8(4);
                self.u16(release.0);
            }
        }
    }

    fn record(&mut self, record: &Record) {
        let paros = usize::from(record.headers.leader_time.is_some());
        self.u16(u16::try_from(paros).expect("a few paros headers"));
        if let Some(time) = record.headers.leader_time {
            self.u16(LEADER_TIME);
            self.bytes(&time.to_le_bytes());
        }
        self.u32_len(record.headers.client.len());
        for header in &record.headers.client {
            self.bytes(&header.key);
            self.bytes(&header.value);
        }
        self.bytes(&record.payload);
    }

    fn nodes(&mut self, nodes: &[NodeId]) {
        self.u32_len(nodes.len());
        for node in nodes {
            self.u64(node.0);
        }
    }

    fn segment(&mut self, record: &SegmentRecord) {
        record.assert_invariants();
        self.u64(record.id().0);
        self.u16(record.release().0);
        let opening = record.opening();
        self.u64(opening.term);
        self.option(opening.expected, |out, id| out.u64(id.0));
        self.u8(opening.grid.f());
        self.u32_len(opening.grid.columns().len());
        for column in opening.grid.columns() {
            self.nodes(column);
        }
        self.nodes(&opening.replicas);
        self.nodes(record.replicas());
        self.u8(match record.state() {
            SegmentState::Pending => 0,
            SegmentState::InForce => 1,
            SegmentState::Sealed => 2,
            SegmentState::Retired => 3,
            SegmentState::Offloaded => 4,
            SegmentState::Trimmed => 5,
        });
        self.option(record.start(), |out, slot| out.u64(slot.0));
        self.option(record.end(), |out, slot| out.u64(slot.0));
        self.option(record.boundary(), |out, boundary| out.boundary(&boundary));
    }

    fn boundary(&mut self, boundary: &BoundaryState) {
        assert!(boundary.is_valid(), "an encoded boundary state is valid");
        self.u64(boundary.next_seq.0);
        self.u64(boundary.first_seq.0);
        self.leader(boundary.leader);
    }
}

/// A decoding in progress: the bytes not read yet.
struct In<'a> {
    rest: &'a [u8],
}

impl<'a> In<'a> {
    fn new(bytes: &'a [u8], kind: u8) -> Result<Self, DecodeError> {
        let mut input = Self { rest: bytes };
        let release = Release(input.u16()?);
        if release != RELEASE {
            return Err(DecodeError::UnknownRelease(release));
        }
        let found = input.u8()?;
        if found != kind {
            return Err(DecodeError::Kind {
                expected: kind,
                found,
            });
        }
        assert!(
            input.rest.len() + HEADER_LEN == bytes.len(),
            "a header is three bytes"
        );
        Ok(input)
    }

    fn finish(&self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Trailing(self.rest.len()))
        }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let (head, rest) = self
            .rest
            .split_first_chunk::<N>()
            .ok_or(DecodeError::Short)?;
        self.rest = rest;
        Ok(*head)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take()?))
    }

    fn u32_len(&mut self) -> Result<usize, DecodeError> {
        let len = u32::from_le_bytes(self.take()?);
        usize::try_from(len).map_err(|_| DecodeError::Short)
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take()?))
    }

    fn u128(&mut self) -> Result<u128, DecodeError> {
        Ok(u128::from_le_bytes(self.take()?))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32_len()?;
        if len > self.rest.len() {
            return Err(DecodeError::Short);
        }
        let (head, rest) = self.rest.split_at(len);
        self.rest = rest;
        Ok(head.to_vec())
    }

    /// A count of items. The decoder allocates nothing for the count: it
    /// reads each item from the bytes that are there, so a count that the
    /// bytes cannot hold ends in [`DecodeError::Short`] at the first item
    /// that is missing.
    fn count(&mut self) -> Result<usize, DecodeError> {
        self.u32_len()
    }

    fn option<T>(
        &mut self,
        what: &'static str,
        some: impl FnOnce(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Option<T>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => some(self).map(Some),
            tag => Err(DecodeError::Tag { what, tag }),
        }
    }

    fn uuid(&mut self) -> Result<LeaderUuid, DecodeError> {
        let uuid = LeaderUuid(self.u128()?);
        if uuid.is_set() {
            Ok(uuid)
        } else {
            Err(DecodeError::Invalid(
                "the unset uuid in a fence or a leader",
            ))
        }
    }

    fn leader(&mut self) -> Result<Leader, DecodeError> {
        match self.u8()? {
            0 => Ok(Leader::Unset),
            1 => Ok(Leader::Multi),
            2 => Ok(Leader::uuid(self.uuid()?)),
            tag => Err(DecodeError::Tag {
                what: "leader",
                tag,
            }),
        }
    }

    fn command(&mut self) -> Result<Command, DecodeError> {
        match self.u8()? {
            0 => Ok(Command::Noop),
            1 => {
                let fence = match self.u8()? {
                    0 => Fence::Open,
                    1 => Fence::Leader {
                        uuid: self.uuid()?,
                        expected_seq: Seq(self.u64()?),
                    },
                    tag => return Err(DecodeError::Tag { what: "fence", tag }),
                };
                let count = self.count()?;
                let records = (0..count)
                    .map(|_| self.record())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::Write(Write { fence, records }))
            }
            2 => Ok(Command::Truncate {
                fence: self.option("truncate fence", Self::uuid)?,
                up_to: Seq(self.u64()?),
            }),
            3 => Ok(Command::SetLeader {
                new: self.option("new leader", Self::uuid)?,
                old: self.leader()?,
            }),
            4 => Ok(Command::Upgrade {
                release: Release(self.u16()?),
            }),
            tag => Err(DecodeError::Tag {
                what: "command",
                tag,
            }),
        }
    }

    fn record(&mut self) -> Result<Record, DecodeError> {
        let paros = self.u16()?;
        let mut headers = Headers::default();
        for _ in 0..paros {
            let key = self.u16()?;
            let value = self.bytes()?;
            match key {
                LEADER_TIME => {
                    if headers.leader_time.is_some() {
                        return Err(DecodeError::Invalid("the leader time twice"));
                    }
                    let time: [u8; 8] = value
                        .try_into()
                        .map_err(|_| DecodeError::Invalid("a leader time is 8 bytes"))?;
                    headers.leader_time = Some(u64::from_le_bytes(time));
                }
                other => return Err(DecodeError::ParosKey(other)),
            }
        }
        let count = self.count()?;
        for _ in 0..count {
            let key = self.bytes()?;
            let value = self.bytes()?;
            headers.client.push(Header { key, value });
        }
        let payload = self.bytes()?;
        Ok(Record { headers, payload })
    }

    fn nodes(&mut self) -> Result<Vec<NodeId>, DecodeError> {
        let count = self.count()?;
        (0..count).map(|_| self.u64().map(NodeId)).collect()
    }

    fn segment(&mut self) -> Result<SegmentRecord, DecodeError> {
        let id = SegmentId(self.u64()?);
        let release = Release(self.u16()?);
        if release != RELEASE {
            return Err(DecodeError::UnknownRelease(release));
        }
        let term = self.u64()?;
        let expected = self.option("expected segment", |input| input.u64().map(SegmentId))?;
        let f = self.u8()?;
        let columns = self.count()?;
        let columns = (0..columns)
            .map(|_| self.nodes())
            .collect::<Result<Vec<_>, _>>()?;
        let grid = Grid::new(f, columns).map_err(|_| DecodeError::Invalid("not a v1 grid"))?;
        let opening = OpeningCall {
            term,
            expected,
            grid,
            replicas: self.nodes()?,
        };
        let replicas = self.nodes()?;
        let state = match self.u8()? {
            0 => SegmentState::Pending,
            1 => SegmentState::InForce,
            2 => SegmentState::Sealed,
            3 => SegmentState::Retired,
            4 => SegmentState::Offloaded,
            5 => SegmentState::Trimmed,
            tag => {
                return Err(DecodeError::Tag {
                    what: "segment state",
                    tag,
                });
            }
        };
        let start = self.option("start", |input| input.u64().map(Slot))?;
        let end = self.option("end", |input| input.u64().map(Slot))?;
        let boundary = self.option("boundary", Self::boundary)?;
        SegmentRecord::from_parts(id, release, opening, replicas, state, start, end, boundary)
            .map_err(|_| DecodeError::Invalid("the fields of a segment record disagree"))
    }

    fn boundary(&mut self) -> Result<BoundaryState, DecodeError> {
        let boundary = BoundaryState {
            next_seq: Seq(self.u64()?),
            first_seq: Seq(self.u64()?),
            leader: self.leader()?,
        };
        if boundary.is_valid() {
            Ok(boundary)
        } else {
            Err(DecodeError::Invalid("first_seq above next_seq"))
        }
    }
}

#[cfg(test)]
mod tests;
