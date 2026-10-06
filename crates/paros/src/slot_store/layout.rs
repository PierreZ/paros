//! The bytes on disk: the slot record (one copy per block), the frame header
//! in front of every payload, and the batch head in front of every batch.
//!
//! Every record carries its own identity inside its checksummed region and
//! is re-derived on every read (TigerBeetle's `header_ok`): a record whose
//! CRC passes but which names another slot, copy, segment or offset is a
//! misdirected read or write, and decodes as damage. The CRC is checked
//! before any other field is trusted.
//!
//! ```text
//! slot record (one per block, RECORD_LEN significant bytes, the rest zero)
//! 0   u32 magic "PRSR"   4  u8 version  5 u8 copy  6 u8 state  7 u8 0
//! 8   u64 slot           16 u64 generation
//! 24  u64 ballot round   32 u64 ballot node
//! 40  u64 batch          48 u64 payload segment   56 u64 payload offset
//! 64  u32 payload len    68 u32 payload crc       72..88 zero
//! 88  u32 crc (0..88)    92 u32 zero
//!
//! frame header (FRAME_LEN bytes; an entry's payload follows it)
//! 0   u32 magic "PRFE" (entry) or "PRBH" (batch head)   4 u32 version
//! 8   u64 segment        16 u64 offset of this header in the segment
//! 24  u64 batch
//! 32  u64 slot           | head: frames in the batch
//! 40  u64 generation     | head: chosen index + 1 (0: none)
//! 48  u64 ballot round   | head: the batch's length in bytes
//! 56  u64 ballot node    | head: zero
//! 64  u32 payload len    68 u32 payload crc       72..88 zero (head: zero)
//! 88  u32 crc (0..88)    92 u32 zero
//! ```

use paros_core::{Ballot, NodeId, Slot};

/// Significant bytes of a slot record; the rest of its block is zero.
pub(crate) const RECORD_LEN: usize = 96;
/// Bytes of a frame header (an entry's or a batch head's).
pub(crate) const FRAME_LEN: usize = 96;
/// Bytes covered by a record's or a frame's CRC.
const CRC_AT: usize = 88;

const RECORD_MAGIC: u32 = u32::from_le_bytes(*b"PRSR");
const ENTRY_MAGIC: u32 = u32::from_le_bytes(*b"PRFE");
const HEAD_MAGIC: u32 = u32::from_le_bytes(*b"PRBH");
/// Bumped when a record's layout changes; another version decodes as damage.
const VERSION: u8 = 1;

const STATE_RESERVED: u8 = 1;
const STATE_PERSISTED: u8 = 2;

// The CRC sits after every field it covers and inside the record.
const _: () = assert!(CRC_AT + 8 == RECORD_LEN);
const _: () = assert!(CRC_AT + 8 == FRAME_LEN);
// A frame header and a slot record fit the smallest block the store accepts.
const _: () = assert!(RECORD_LEN <= super::MIN_BLOCK);
const _: () = assert!(FRAME_LEN <= super::MIN_BLOCK);

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4-byte field"))
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8-byte field"))
}

fn put32(out: &mut [u8], at: usize, value: u32) {
    out[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put64(out: &mut [u8], at: usize, value: u64) {
    out[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

fn seal_crc(out: &mut [u8]) {
    let crc = crc32c::crc32c(&out[..CRC_AT]);
    put32(out, CRC_AT, crc);
}

fn crc_ok(bytes: &[u8]) -> bool {
    bytes.len() >= CRC_AT + 8
        && crc32c::crc32c(&bytes[..CRC_AT]) == u32_at(bytes, CRC_AT)
        && u32_at(bytes, CRC_AT + 4) == 0
}

/// Where a payload lives: its segment, the offset of its frame header, its
/// length and CRC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Loc {
    pub segment: u64,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
}

impl Loc {
    /// Where the payload's bytes start in its segment.
    pub(crate) fn payload_at(self) -> u64 {
        self.offset + FRAME_LEN as u64
    }
}

/// A slot's persist record: the write it records and where its payload is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotRecord {
    pub slot: Slot,
    pub generation: u64,
    pub ballot: Ballot,
    pub batch: u64,
    pub loc: Loc,
}

/// What one copy of a slot record turned out to hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Copy {
    /// The reserved record the chunk was formatted with: positively nothing.
    Reserved,
    /// A persist record naming this slot and copy.
    Persisted(SlotRecord),
    /// Anything else: torn, rotted, zeroed, stale, or misdirected.
    Bad,
}

/// Encode the reserved record of `copy` of `slot` into `out` (one block).
pub(crate) fn encode_reserved(slot: Slot, copy: u8, out: &mut [u8]) {
    out.fill(0);
    put32(out, 0, RECORD_MAGIC);
    out[4] = VERSION;
    out[5] = copy;
    out[6] = STATE_RESERVED;
    put64(out, 8, slot.0);
    seal_crc(out);
    // Pair of `decode_copy`: a formatted copy reads back as reserved.
    assert!(
        decode_copy(out, slot, copy) == Copy::Reserved,
        "a reserved record reads back as reserved"
    );
}

/// Encode `record` as `copy` of its slot into `out` (one block).
pub(crate) fn encode_record(record: &SlotRecord, copy: u8, out: &mut [u8]) {
    assert!(copy < 2, "a slot has two copies");
    assert!(record.generation > 0, "generation zero is the reserved record");
    out.fill(0);
    put32(out, 0, RECORD_MAGIC);
    out[4] = VERSION;
    out[5] = copy;
    out[6] = STATE_PERSISTED;
    put64(out, 8, record.slot.0);
    put64(out, 16, record.generation);
    put64(out, 24, record.ballot.round);
    put64(out, 32, record.ballot.node.0);
    put64(out, 40, record.batch);
    put64(out, 48, record.loc.segment);
    put64(out, 56, record.loc.offset);
    put32(out, 64, record.loc.len);
    put32(out, 68, record.loc.crc);
    seal_crc(out);
    // Pair of `decode_copy`: the record a sync writes is the record a boot
    // reads.
    assert!(
        decode_copy(out, record.slot, copy) == Copy::Persisted(*record),
        "a slot record reads back as itself"
    );
}

/// Classify the bytes stored for `copy` of `slot`.
pub(crate) fn decode_copy(bytes: &[u8], slot: Slot, copy: u8) -> Copy {
    if bytes.len() < RECORD_LEN || !crc_ok(bytes) {
        return Copy::Bad;
    }
    if u32_at(bytes, 0) != RECORD_MAGIC
        || bytes[4] != VERSION
        || bytes[5] != copy
        || bytes[7] != 0
        || u64_at(bytes, 8) != slot.0
        || bytes[72..CRC_AT].iter().any(|b| *b != 0)
    {
        return Copy::Bad;
    }
    match bytes[6] {
        STATE_RESERVED => {
            let zero = bytes[16..72].iter().all(|b| *b == 0);
            if zero { Copy::Reserved } else { Copy::Bad }
        }
        STATE_PERSISTED if u64_at(bytes, 16) > 0 => Copy::Persisted(SlotRecord {
            slot,
            generation: u64_at(bytes, 16),
            ballot: Ballot {
                round: u64_at(bytes, 24),
                node: NodeId(u64_at(bytes, 32)),
            },
            batch: u64_at(bytes, 40),
            loc: Loc {
                segment: u64_at(bytes, 48),
                offset: u64_at(bytes, 56),
                len: u32_at(bytes, 64),
                crc: u32_at(bytes, 68),
            },
        }),
        _ => Copy::Bad,
    }
}

/// An entry's frame header: the redundant identifier kept beside the
/// payload (CLSTORE §3.3.4), so a write whose slot record was lost is still
/// identified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Frame {
    pub slot: Slot,
    pub generation: u64,
    pub ballot: Ballot,
    pub batch: u64,
    pub loc: Loc,
}

/// A batch head: the batch's number, its frame count, its length, and the
/// chosen index as of the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Head {
    pub segment: u64,
    pub offset: u64,
    pub batch: u64,
    pub frames: u64,
    pub chosen: Option<Slot>,
    pub bytes: u64,
}

/// Encode an entry's frame header into `out` (exactly [`FRAME_LEN`] bytes).
pub(crate) fn encode_frame(frame: &Frame, out: &mut [u8]) {
    assert!(out.len() == FRAME_LEN, "a frame header is FRAME_LEN bytes");
    assert!(frame.generation > 0, "an entry spends a generation");
    out.fill(0);
    put32(out, 0, ENTRY_MAGIC);
    put32(out, 4, u32::from(VERSION));
    put64(out, 8, frame.loc.segment);
    put64(out, 16, frame.loc.offset);
    put64(out, 24, frame.batch);
    put64(out, 32, frame.slot.0);
    put64(out, 40, frame.generation);
    put64(out, 48, frame.ballot.round);
    put64(out, 56, frame.ballot.node.0);
    put32(out, 64, frame.loc.len);
    put32(out, 68, frame.loc.crc);
    seal_crc(out);
    assert!(
        decode_frame(out, frame.loc.segment, frame.loc.offset) == Some(*frame),
        "a frame header reads back as itself"
    );
}

/// Decode the entry frame header stored at `offset` of `segment`.
pub(crate) fn decode_frame(bytes: &[u8], segment: u64, offset: u64) -> Option<Frame> {
    if bytes.len() < FRAME_LEN || !crc_ok(bytes) || u32_at(bytes, 0) != ENTRY_MAGIC {
        return None;
    }
    let identity = u32_at(bytes, 4) == u32::from(VERSION)
        && u64_at(bytes, 8) == segment
        && u64_at(bytes, 16) == offset
        && u64_at(bytes, 40) > 0
        && bytes[72..CRC_AT].iter().all(|b| *b == 0);
    identity.then(|| Frame {
        slot: Slot(u64_at(bytes, 32)),
        generation: u64_at(bytes, 40),
        ballot: Ballot {
            round: u64_at(bytes, 48),
            node: NodeId(u64_at(bytes, 56)),
        },
        batch: u64_at(bytes, 24),
        loc: Loc {
            segment,
            offset,
            len: u32_at(bytes, 64),
            crc: u32_at(bytes, 68),
        },
    })
}

/// Encode a batch head into `out` (exactly [`FRAME_LEN`] bytes).
pub(crate) fn encode_head(head: &Head, out: &mut [u8]) {
    assert!(out.len() == FRAME_LEN, "a batch head is FRAME_LEN bytes");
    assert!(head.bytes >= FRAME_LEN as u64, "a batch holds its own head");
    out.fill(0);
    put32(out, 0, HEAD_MAGIC);
    put32(out, 4, u32::from(VERSION));
    put64(out, 8, head.segment);
    put64(out, 16, head.offset);
    put64(out, 24, head.batch);
    put64(out, 32, head.frames);
    put64(out, 40, head.chosen.map_or(0, |slot| slot.0 + 1));
    put64(out, 48, head.bytes);
    seal_crc(out);
    assert!(
        decode_head(out, head.segment, head.offset) == Some(*head),
        "a batch head reads back as itself"
    );
}

/// Decode the batch head stored at `offset` of `segment`.
pub(crate) fn decode_head(bytes: &[u8], segment: u64, offset: u64) -> Option<Head> {
    if bytes.len() < FRAME_LEN || !crc_ok(bytes) || u32_at(bytes, 0) != HEAD_MAGIC {
        return None;
    }
    let identity = u32_at(bytes, 4) == u32::from(VERSION)
        && u64_at(bytes, 8) == segment
        && u64_at(bytes, 16) == offset
        && u64_at(bytes, 48) >= FRAME_LEN as u64
        && bytes[56..CRC_AT].iter().all(|b| *b == 0);
    identity.then(|| Head {
        segment,
        offset,
        batch: u64_at(bytes, 24),
        frames: u64_at(bytes, 32),
        chosen: u64_at(bytes, 40).checked_sub(1).map(Slot),
        bytes: u64_at(bytes, 48),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> SlotRecord {
        SlotRecord {
            slot: Slot(77),
            generation: 3,
            ballot: Ballot {
                round: 9,
                node: NodeId(2),
            },
            batch: 12,
            loc: Loc {
                segment: 4,
                offset: 1024,
                len: 33,
                crc: 0xDEAD_BEEF,
            },
        }
    }

    #[test]
    fn a_record_names_its_slot_and_copy() {
        let mut block = vec![0; 512];
        encode_record(&record(), 1, &mut block);
        assert_eq!(decode_copy(&block, Slot(77), 1), Copy::Persisted(record()));
        assert_eq!(decode_copy(&block, Slot(78), 1), Copy::Bad, "misdirected slot");
        assert_eq!(decode_copy(&block, Slot(77), 0), Copy::Bad, "misdirected copy");
        block[20] ^= 1;
        assert_eq!(decode_copy(&block, Slot(77), 1), Copy::Bad, "a flipped bit");
    }

    #[test]
    fn zeros_are_damage_never_empty() {
        let block = vec![0; 512];
        assert_eq!(decode_copy(&block, Slot(0), 0), Copy::Bad);
        let mut reserved = vec![0; 512];
        encode_reserved(Slot(5), 0, &mut reserved);
        assert_eq!(decode_copy(&reserved, Slot(5), 0), Copy::Reserved);
        assert_eq!(decode_copy(&reserved, Slot(6), 0), Copy::Bad);
    }

    #[test]
    fn frames_and_heads_name_their_place() {
        let r = record();
        let frame = Frame {
            slot: r.slot,
            generation: r.generation,
            ballot: r.ballot,
            batch: r.batch,
            loc: r.loc,
        };
        let mut bytes = [0; FRAME_LEN];
        encode_frame(&frame, &mut bytes);
        assert_eq!(decode_frame(&bytes, 4, 1024), Some(frame));
        assert_eq!(decode_frame(&bytes, 4, 512), None, "misdirected offset");
        assert_eq!(decode_head(&bytes, 4, 1024), None, "an entry is no head");
        let head = Head {
            segment: 4,
            offset: 0,
            batch: 12,
            frames: 2,
            chosen: Some(Slot(0)),
            bytes: 400,
        };
        encode_head(&head, &mut bytes);
        assert_eq!(decode_head(&bytes, 4, 0), Some(head));
        assert_eq!(decode_frame(&bytes, 4, 0), None, "a head is no entry");
    }
}
