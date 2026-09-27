//! How one store record becomes one journal entry: the epoch (the record's
//! kind), the tag (the record's identity, kept in the journal's
//! far identifier), and the payload (a version byte and the record's
//! `postcard` encoding).
//!
//! The identity lives in the identifier because that is what survives when
//! the entry's bytes do not: a damaged accepted record still names its slot
//! and ballot, a damaged snapshot chunk its point and index. The payload
//! repeats the identity, and decoding checks the two agree — a record that
//! checks out but names something other than its identifier is a
//! misdirected write, and is treated exactly like a damaged one.

use moonpool_journal::{Entry, EntryId, TAG_SIZE, Tag};
use paros_core::{Ballot, NodeId, Slot};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Bumped when a record's encoding changes; an entry of another version is
/// not decoded (it reads as damaged).
const FORMAT_VERSION: u8 = 1;

/// What an entry holds, as its epoch records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Kind {
    /// An accepted (or learned) record: `(slot, ballot, command)`.
    Accepted = 1,
    /// A slot whose value is lost but whose identity survived, re-emitted
    /// by a checkpoint.
    Faulty = 2,
    /// The chosen index.
    ChosenIndex = 3,
    /// A truncation: the floor and the sealed ledger it drops.
    Truncate = 4,
    /// A snapshot install: the boundary and the peer's sealed ledger.
    InstallSnapshot = 5,
    /// A decided snapshot point's header: length and per-chunk checksums.
    SnapPoint = 6,
    /// One chunk of a decided snapshot point.
    SnapChunk = 7,
    /// Part of a checkpoint's sealed ledger.
    Sealed = 8,
    /// A matchmaker registration.
    Register = 16,
    /// The matchmaker's durable scalars, whole.
    Scalars = 17,
    /// A checkpoint opens: what follows up to its end is the whole image.
    Begin = 128,
    /// A checkpoint — or a matchmaker's registry install — closes.
    End = 129,
    /// A matchmaker's registry install opens: the successor generation's
    /// scalars, then its registrations, then an `End`. Not a checkpoint: it
    /// replaces the state rather than copying it.
    Install = 130,
}

impl Kind {
    fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => Kind::Accepted,
            2 => Kind::Faulty,
            3 => Kind::ChosenIndex,
            4 => Kind::Truncate,
            5 => Kind::InstallSnapshot,
            6 => Kind::SnapPoint,
            7 => Kind::SnapChunk,
            8 => Kind::Sealed,
            16 => Kind::Register,
            17 => Kind::Scalars,
            128 => Kind::Begin,
            129 => Kind::End,
            130 => Kind::Install,
            _ => return None,
        })
    }
}

/// The epoch an entry of `kind` gets: the kind itself, so the journal's
/// far identifier alone says what a damaged entry was.
pub(crate) fn epoch(kind: Kind) -> u64 {
    kind as u64
}

/// The kind an epoch records, if it is one this store writes.
pub(crate) fn kind_of(epoch: u64) -> Option<Kind> {
    Kind::from_byte(u8::try_from(epoch).ok()?)
}

/// A tag of up to three little-endian words.
pub(crate) fn tag(words: [u64; 3]) -> Tag {
    let mut tag = [0; TAG_SIZE];
    for (at, word) in words.iter().enumerate() {
        tag[at * 8..at * 8 + 8].copy_from_slice(&word.to_le_bytes());
    }
    tag
}

/// The three words of a tag.
pub(crate) fn words(tag: &Tag) -> [u64; 3] {
    let word = |at: usize| u64::from_le_bytes(tag[at * 8..at * 8 + 8].try_into().expect("8 bytes"));
    [word(0), word(1), word(2)]
}

/// The tag of a slot's record: its slot and the ballot it carries.
pub(crate) fn slot_tag(slot: Slot, ballot: Ballot) -> Tag {
    tag([slot.0, ballot.round, ballot.node.0])
}

/// The `(slot, ballot)` a slot record's tag names.
pub(crate) fn slot_identity(tag: &Tag) -> (Slot, Ballot) {
    let [slot, round, node] = words(tag);
    (
        Slot(slot),
        Ballot {
            round,
            node: NodeId(node),
        },
    )
}

/// A record as the journal stores it.
pub(crate) trait Framed: Serialize + DeserializeOwned {
    /// The kind of entry this record is.
    fn kind(&self) -> Kind;
    /// The identity kept in the far identifier.
    fn tag(&self) -> Tag;
}

/// Encode a record's payload.
pub(crate) fn encode<R: Framed>(record: &R) -> Vec<u8> {
    let mut bytes = vec![FORMAT_VERSION];
    bytes.extend(postcard::to_stdvec(record).expect("in-memory encoding of a store record"));
    bytes
}

/// Decode an intact entry, or `None` when its payload is not a record of
/// this version whose kind and identity agree with its identifier — a
/// misdirected or foreign entry, treated as damaged.
pub(crate) fn decode<R: Framed>(entry: &Entry) -> Option<R> {
    let (&version, body) = entry.payload.split_first()?;
    if version != FORMAT_VERSION {
        return None;
    }
    let record: R = postcard::from_bytes(body).ok()?;
    (kind_of(entry.epoch) == Some(record.kind()) && record.tag() == entry.tag).then_some(record)
}

/// One scanned entry: its identity and, when intact, its record.
pub(crate) struct Scanned<R> {
    /// The identity the journal's identifier records.
    pub id: EntryId,
    /// The kind the epoch records (`None` for an epoch this store never
    /// writes).
    pub kind: Option<Kind>,
    /// The record, or `None` when the entry is damaged or misdirected.
    pub record: Option<R>,
}

impl<R: Framed> Scanned<R> {
    /// Classify one entry of a replay.
    pub(crate) fn new(entry: Result<Entry, EntryId>) -> Self {
        match entry {
            Ok(entry) => {
                let record = decode(&entry);
                Self {
                    id: EntryId {
                        index: entry.index,
                        epoch: entry.epoch,
                        tag: entry.tag,
                    },
                    kind: kind_of(entry.epoch),
                    record,
                }
            }
            Err(id) => Self {
                kind: kind_of(id.epoch),
                id,
                record: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_carry_the_kind() {
        assert_eq!(kind_of(epoch(Kind::SnapChunk)), Some(Kind::SnapChunk));
        assert_eq!(kind_of(1 << 56), None, "a foreign epoch is no kind");
        assert_eq!(kind_of(0), None, "an all-zero epoch is no kind");
    }

    #[test]
    fn a_slot_tag_names_the_slot_and_the_ballot() {
        let ballot = Ballot {
            round: 7,
            node: NodeId(1003),
        };
        assert_eq!(slot_identity(&slot_tag(Slot(9), ballot)), (Slot(9), ballot));
    }
}
