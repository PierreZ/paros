//! The **election record**: what the election writes in the journal.
//!
//! A `SetLeader` takes no position in the journal, so a reader never sees
//! one. What a reader sees are the records the holder writes under its uuid:
//!
//! - the **term record**, the first write of a term: it names the term's
//!   number, the holder's uuid and the holder's interface;
//! - the **renewal**, a write of an idle holder: the same fields.
//!
//! The number of a term is the count of the terms in the fold (§3.3): the
//! number of the last election record, plus one, read up to the tail that
//! the answer of the winning `SetLeader` names.
//!
//! The layout. An election record carries exactly one client header whose
//! key is [`KEY`]. Its value is 26 bytes: a version byte (`1`), a kind byte
//! (`1` for a term record, `2` for a renewal), the term number as a `u64`
//! little endian, and the uuid as a `u128` little endian. The payload is the
//! interface, opaque bytes. A record with no such header is a record of the
//! program and the election ignores it; a record whose header is malformed
//! is data from the journal, never a panic: the fold ignores it too.

use paros_journal::{Header, Headers, LeaderUuid, Record};

/// The key of the client header of an election record.
pub const KEY: &[u8] = b"paros.election";

/// The version byte of the layout.
const VERSION: u8 = 1;
/// The length of the header value.
const VALUE_LEN: usize = 26;

/// What an election record says.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// The first write of a term.
    Term,
    /// A write of an idle holder.
    Renewal,
}

impl Kind {
    const fn byte(self) -> u8 {
        match self {
            Self::Term => 1,
            Self::Renewal => 2,
        }
    }

    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Term),
            2 => Some(Self::Renewal),
            _ => None,
        }
    }
}

/// The number of a term: the count of the terms in the fold. The first term
/// is 1; 0 is the number before any term.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

impl Term {
    /// The number of the next term.
    ///
    /// # Panics
    ///
    /// When the number would overflow, which no journal reaches.
    #[must_use]
    pub fn next(self) -> Self {
        Self(
            self.0
                .checked_add(1)
                .expect("a term number never overflows"),
        )
    }
}

/// One election record, decoded.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ElectionRecord {
    /// A term record or a renewal.
    pub kind: Kind,
    /// The number of the holder's term.
    pub term: Term,
    /// The uuid the holder writes under.
    pub uuid: LeaderUuid,
    /// The holder's interface: its address and its `InterfaceRef`, opaque.
    pub interface: Vec<u8>,
}

impl ElectionRecord {
    /// The record as the journal keeps it.
    ///
    /// # Panics
    ///
    /// When the uuid is unset (it never leads) or the term is 0.
    #[must_use]
    pub fn encode(&self) -> Record {
        assert!(self.uuid.is_set(), "an election record names a set uuid");
        assert!(self.term.0 > 0, "an election record names a term");
        let mut value = Vec::with_capacity(VALUE_LEN);
        value.push(VERSION);
        value.push(self.kind.byte());
        value.extend_from_slice(&self.term.0.to_le_bytes());
        value.extend_from_slice(&self.uuid.0.to_le_bytes());
        assert!(value.len() == VALUE_LEN, "the value has its fixed length");
        let record = Record {
            headers: Headers {
                leader_time: None,
                client: vec![Header {
                    key: KEY.to_vec(),
                    value,
                }],
            },
            payload: self.interface.clone(),
        };
        assert!(
            Self::decode(&record).as_ref() == Some(self),
            "an encoded record decodes to itself"
        );
        record
    }

    /// The election record that `record` holds, or `None` for a record of
    /// the program or a malformed one.
    #[must_use]
    pub fn decode(record: &Record) -> Option<Self> {
        let mut ours = record.headers.client.iter().filter(|h| h.key == KEY);
        let header = ours.next()?;
        if ours.next().is_some() || header.value.len() != VALUE_LEN || header.value[0] != VERSION {
            return None;
        }
        let kind = Kind::from_byte(header.value[1])?;
        let term = Term(u64::from_le_bytes(header.value[2..10].try_into().ok()?));
        let uuid = LeaderUuid(u128::from_le_bytes(header.value[10..26].try_into().ok()?));
        if term.0 == 0 || !uuid.is_set() {
            return None;
        }
        Some(Self {
            kind,
            term,
            uuid,
            interface: record.payload.clone(),
        })
    }

    /// Whether `record` carries the key of an election record, well formed
    /// or not: the program may not write one.
    #[must_use]
    pub fn reserved(record: &Record) -> bool {
        record.headers.client.iter().any(|h| h.key == KEY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ElectionRecord {
        ElectionRecord {
            kind: Kind::Term,
            term: Term(3),
            uuid: LeaderUuid(0xABCD),
            interface: b"10.0.0.1:7000/ref 4".to_vec(),
        }
    }

    #[test]
    fn a_record_round_trips() {
        let record = sample().encode();
        assert_eq!(ElectionRecord::decode(&record), Some(sample()));
        assert!(ElectionRecord::reserved(&record));
        let renewal = ElectionRecord {
            kind: Kind::Renewal,
            ..sample()
        };
        assert_eq!(ElectionRecord::decode(&renewal.encode()), Some(renewal));
    }

    #[test]
    fn the_layout_is_the_documented_one() {
        let record = sample().encode();
        let value = &record.headers.client[0].value;
        assert_eq!(value[0], 1, "version");
        assert_eq!(value[1], 1, "a term record");
        assert_eq!(&value[2..10], &3u64.to_le_bytes());
        assert_eq!(&value[10..26], &0xABCDu128.to_le_bytes());
    }

    #[test]
    fn a_program_record_is_not_an_election_record() {
        let record = Record::new(b"hello".to_vec());
        assert_eq!(ElectionRecord::decode(&record), None);
        assert!(!ElectionRecord::reserved(&record));
    }

    #[test]
    fn a_malformed_record_is_ignored_never_a_panic() {
        let mut record = sample().encode();
        record.headers.client[0].value.pop();
        assert_eq!(ElectionRecord::decode(&record), None, "short");
        let mut record = sample().encode();
        record.headers.client[0].value[0] = 9;
        assert_eq!(ElectionRecord::decode(&record), None, "version");
        let mut record = sample().encode();
        record.headers.client[0].value[1] = 7;
        assert_eq!(ElectionRecord::decode(&record), None, "kind");
        let mut record = sample().encode();
        record.headers.client[0].value[2..10].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(ElectionRecord::decode(&record), None, "term 0");
        let mut record = sample().encode();
        record.headers.client[0].value[10..26].copy_from_slice(&0u128.to_le_bytes());
        assert_eq!(ElectionRecord::decode(&record), None, "unset uuid");
        let mut record = sample().encode();
        let copy = record.headers.client[0].clone();
        record.headers.client.push(copy);
        assert_eq!(ElectionRecord::decode(&record), None, "two headers");
        assert!(ElectionRecord::reserved(&record));
    }
}
