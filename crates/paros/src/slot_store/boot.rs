//! The boot fold: from the bytes on disk to what each slot holds.
//!
//! Pure functions over what the boot read, so every row of the module
//! doc's table is unit-testable without a disk. [`scan_segment`] walks one
//! payload segment and collects every batch head and entry frame whose
//! header checks out; [`fold_slot`] decides one slot from its two record
//! copies and the frames that name it.

use std::collections::BTreeMap;

use paros_core::{Ballot, Command, Slot};

use super::layout::{
    Copy, FRAME_LEN, Frame, Head, Loc, SlotRecord, decode_frame, decode_head,
};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{StorageError, StorageRecord};

/// One entry frame the scan found: its header, and whether its payload
/// checked out against the header's CRC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    pub frame: Frame,
    pub payload_ok: bool,
}

/// What a segment scan found.
#[derive(Debug, Default)]
pub(crate) struct Scan {
    pub heads: Vec<Head>,
    pub frames: Vec<Seen>,
}

/// Walk segment `segment` (its bytes, whole blocks): a batch starts on a
/// block boundary with its head, its frames follow packed, and it is padded
/// to the next block. A damaged head costs one block of the walk; a damaged
/// frame header costs the rest of its batch (whose frames the slot records
/// still point at); the walk resumes at the batch's declared end.
pub(crate) fn scan_segment(segment: u64, bytes: &[u8], block: usize) -> Scan {
    assert!(block >= FRAME_LEN, "a block holds a frame header");
    let mut scan = Scan::default();
    let mut at = 0_usize;
    while at + FRAME_LEN <= bytes.len() {
        let Some(head) = decode_head(&bytes[at..at + FRAME_LEN], segment, at as u64) else {
            at += block;
            continue;
        };
        let end = usize::try_from(head.bytes)
            .ok()
            .and_then(|len| at.checked_add(len))
            .filter(|end| *end <= bytes.len());
        let Some(end) = end else {
            // A head claiming more than the file holds: the tail of a batch
            // a crash cut, or damage. Nothing past it is this batch's.
            at += block;
            continue;
        };
        scan.heads.push(head);
        let mut p = at + FRAME_LEN;
        for _ in 0..head.frames {
            if p + FRAME_LEN > end {
                break;
            }
            let Some(frame) = decode_frame(&bytes[p..p + FRAME_LEN], segment, p as u64) else {
                break;
            };
            let from = p + FRAME_LEN;
            let to = from + frame.loc.len as usize;
            if to > end || frame.batch != head.batch {
                break;
            }
            let payload_ok = crc32c::crc32c(&bytes[from..to]) == frame.loc.crc;
            scan.frames.push(Seen { frame, payload_ok });
            p = to;
        }
        at = end.next_multiple_of(block);
    }
    // Everything the walk kept sits inside the segment it walked.
    assert!(
        scan.frames.iter().all(|seen| seen.frame.loc.segment == segment),
        "a scanned frame belongs to its segment"
    );
    scan
}

/// The payload bytes `loc` names, if its segment is on disk and they check
/// out against `loc`'s CRC.
pub(crate) fn payload<'a>(segments: &'a BTreeMap<u64, Vec<u8>>, loc: Loc) -> Option<&'a [u8]> {
    let bytes = segments.get(&loc.segment)?;
    let from = usize::try_from(loc.payload_at()).ok()?;
    let to = from.checked_add(loc.len as usize)?;
    let payload = bytes.get(from..to)?;
    (crc32c::crc32c(payload) == loc.crc).then_some(payload)
}

/// How a slot came out of the boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The slot holds this vote, read from its payload at `loc`.
    Accepted {
        ballot: Ballot,
        command: Command,
        loc: Loc,
        /// The vote's frame landed without its slot record: a crash cut the
        /// batch after its payload sync (the CTRL undecidable row), and the
        /// store keeps the landed vote.
        unrecorded: bool,
    },
    /// The vote's identity survived and its value did not: CTRL's faulty
    /// `(slot, ballot)`, repaired from peers.
    Faulty { ballot: Ballot },
    /// Nothing accepted here (a reserved record, or only torn writes a
    /// crash cut before they were persisted).
    Empty,
}

/// What [`fold_slot`] found for one slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Folded {
    pub outcome: Outcome,
    /// The highest generation any evidence carries: the next write spends
    /// one above it, so no generation is ever written twice.
    pub generation: u64,
    /// The generation each copy holds (`Some(0)`: reserved; `None`: bad).
    pub copies: [Option<u64>; 2],
    /// Torn writes the fold discarded as a crash during the write.
    pub torn: usize,
}

fn misdirected(slot: Slot) -> StorageError {
    StorageError::Corruption {
        record: StorageRecord::Accepted(slot),
        fault: IntegrityFault::Misdirected,
        verdict: CorruptionVerdict::Corrupted,
    }
}

/// Decide one slot (the module doc's table).
///
/// `max_batch` is the newest batch any intact evidence names: a batch
/// before it was synced whole, because a batch starts only after the
/// previous one's sync returned.
///
/// # Errors
///
/// A corruption verdict when two pieces of evidence of one generation
/// disagree (two copies, or a copy and a frame): only damage produces that,
/// and picking one could serve a value the slot never held.
pub(crate) fn fold_slot(
    slot: Slot,
    copies: [Copy; 2],
    frames: &[Seen],
    segments: &BTreeMap<u64, Vec<u8>>,
    max_batch: u64,
    decode: impl Fn(&[u8]) -> Option<Command>,
) -> Result<Folded, StorageError> {
    let records: Vec<SlotRecord> = copies
        .iter()
        .filter_map(|copy| match copy {
            Copy::Persisted(record) => Some(*record),
            _ => None,
        })
        .collect();
    let held = copies.map(|copy| match copy {
        Copy::Reserved => Some(0),
        Copy::Persisted(record) => Some(record.generation),
        Copy::Bad => None,
    });
    if let [a, b] = records.as_slice()
        && a.generation == b.generation
        && a != b
    {
        return Err(misdirected(slot));
    }
    let mut generations: Vec<u64> = records
        .iter()
        .map(|r| r.generation)
        .chain(frames.iter().map(|s| s.frame.generation))
        .collect();
    generations.sort_unstable();
    generations.dedup();
    let generation = generations.last().copied().unwrap_or(0);
    let mut torn = 0;
    for &g in generations.iter().rev() {
        let record = records.iter().find(|r| r.generation == g);
        let at_g: Vec<&Seen> = frames.iter().filter(|s| s.frame.generation == g).collect();
        for seen in &at_g {
            let agrees = record.is_none_or(|r| {
                r.ballot == seen.frame.ballot && r.loc == seen.frame.loc && r.batch == seen.frame.batch
            });
            if !agrees || at_g.iter().any(|other| other.frame != seen.frame) {
                return Err(misdirected(slot));
            }
        }
        let frame = at_g.first().map(|seen| seen.frame);
        let ballot = record.map_or_else(|| frame.expect("a generation has evidence").ballot, |r| r.ballot);
        let loc = record.map_or_else(|| frame.expect("a generation has evidence").loc, |r| r.loc);
        if let Some(command) = payload(segments, loc).and_then(&decode) {
            return Ok(Folded {
                outcome: Outcome::Accepted {
                    ballot,
                    command,
                    loc,
                    unrecorded: record.is_none(),
                },
                generation,
                copies: held,
                torn,
            });
        }
        // The payload is lost. Persisted — a slot record names it, or a
        // later batch proves its batch was synced whole — it is corruption,
        // and its identity is known: faulty. Not persisted, it is a crash
        // during the write: never acknowledged, discarded, and the slot is
        // what the generations before it say.
        let batch = record.map_or_else(|| frame.expect("evidence").batch, |r| r.batch);
        if record.is_some() || batch < max_batch {
            return Ok(Folded {
                outcome: Outcome::Faulty { ballot },
                generation,
                copies: held,
                torn,
            });
        }
        torn += 1;
    }
    Ok(Folded {
        outcome: Outcome::Empty,
        generation,
        copies: held,
        torn,
    })
}

#[cfg(test)]
mod tests {
    use paros_core::{ClientId, Entry, Generation, NodeId, Seq, Value};

    use super::super::layout::{Head, encode_frame, encode_head};
    use super::*;

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(1),
        }
    }

    fn command(byte: u8) -> Command {
        Command::Write(Entry {
            generation: Generation(1),
            owner: ClientId(1),
            seq: Seq(1),
            records: vec![Value(vec![byte])],
        })
    }

    fn decode(bytes: &[u8]) -> Option<Command> {
        Some(command(*bytes.first()?))
    }

    /// One segment (number 0) holding `batches`, each a batch number and
    /// its `(slot, generation, round, byte)` entries, and the frames written.
    fn segment(batches: &[(u64, &[(u64, u64, u64, u8)])]) -> (Vec<u8>, Vec<Frame>) {
        let mut bytes = Vec::new();
        let mut frames = Vec::new();
        for &(batch, entries) in batches {
            let start = bytes.len();
            bytes.resize(start + FRAME_LEN, 0);
            for &(slot, generation, round, byte) in entries {
                let offset = bytes.len() as u64;
                let payload = [byte];
                let frame = Frame {
                    slot: Slot(slot),
                    generation,
                    ballot: ballot(round),
                    batch,
                    loc: Loc {
                        segment: 0,
                        offset,
                        len: 1,
                        crc: crc32c::crc32c(&payload),
                    },
                };
                let mut header = [0; FRAME_LEN];
                encode_frame(&frame, &mut header);
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(&payload);
                frames.push(frame);
            }
            let head = Head {
                segment: 0,
                offset: start as u64,
                batch,
                frames: entries.len() as u64,
                chosen: None,
                bytes: (bytes.len() - start) as u64,
            };
            encode_head(&head, &mut bytes[start..start + FRAME_LEN]);
            bytes.resize(bytes.len().next_multiple_of(512), 0);
        }
        (bytes, frames)
    }

    fn record(frame: &Frame) -> SlotRecord {
        SlotRecord {
            slot: frame.slot,
            generation: frame.generation,
            ballot: frame.ballot,
            batch: frame.batch,
            loc: frame.loc,
        }
    }

    fn seen(bytes: &[u8]) -> Vec<Seen> {
        scan_segment(0, bytes, 512).frames
    }

    #[test]
    fn a_recorded_vote_reads_back() {
        let (bytes, frames) = segment(&[(1, &[(3, 1, 4, 0xA)])]);
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let folded = fold_slot(
            Slot(3),
            [Copy::Persisted(record(&frames[0])), Copy::Reserved],
            &seen(&bytes),
            &segments,
            1,
            decode,
        )
        .expect("folds");
        assert!(matches!(folded.outcome, Outcome::Accepted { unrecorded: false, .. }));
        assert_eq!(folded.copies, [Some(1), Some(0)]);
    }

    #[test]
    fn a_lost_payload_under_a_record_is_faulty_never_empty() {
        let (mut bytes, frames) = segment(&[(1, &[(3, 1, 4, 0xA)])]);
        let at = usize::try_from(frames[0].loc.payload_at()).expect("small");
        bytes[at] ^= 0xFF;
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let folded = fold_slot(
            Slot(3),
            [Copy::Persisted(record(&frames[0])), Copy::Reserved],
            &seen(&bytes),
            &segments,
            1,
            decode,
        )
        .expect("folds");
        assert_eq!(folded.outcome, Outcome::Faulty { ballot: ballot(4) });
    }

    #[test]
    fn a_torn_unrecorded_write_falls_back_to_the_previous_vote() {
        // Generation 1 recorded in batch 1; generation 2 torn in batch 2,
        // its slot record never written.
        let (mut bytes, frames) = segment(&[(1, &[(3, 1, 4, 0xA)]), (2, &[(3, 2, 5, 0xB)])]);
        let at = usize::try_from(frames[1].loc.payload_at()).expect("small");
        bytes[at] ^= 0xFF;
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let folded = fold_slot(
            Slot(3),
            [Copy::Persisted(record(&frames[0])), Copy::Reserved],
            &seen(&bytes),
            &segments,
            2,
            decode,
        )
        .expect("folds");
        assert!(
            matches!(&folded.outcome, Outcome::Accepted { ballot: b, .. } if *b == ballot(4)),
            "the vote before the torn write: {folded:?}"
        );
        assert_eq!(folded.torn, 1);
        assert_eq!(folded.generation, 2, "the torn generation is never reused");
    }

    #[test]
    fn a_landed_unrecorded_vote_is_kept() {
        let (bytes, _) = segment(&[(2, &[(3, 1, 5, 0xB)])]);
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let folded = fold_slot(
            Slot(3),
            [Copy::Reserved, Copy::Reserved],
            &seen(&bytes),
            &segments,
            2,
            decode,
        )
        .expect("folds");
        assert!(matches!(folded.outcome, Outcome::Accepted { unrecorded: true, .. }));
    }

    #[test]
    fn a_lost_record_and_payload_of_a_synced_batch_stay_identified() {
        // Batch 1 was synced (batch 2 exists): its record is gone and its
        // payload rotted. The frame header still names the vote.
        let (mut bytes, frames) = segment(&[(1, &[(3, 1, 4, 0xA)])]);
        let at = usize::try_from(frames[0].loc.payload_at()).expect("small");
        bytes[at] ^= 0xFF;
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let folded = fold_slot(
            Slot(3),
            [Copy::Bad, Copy::Reserved],
            &seen(&bytes),
            &segments,
            2,
            decode,
        )
        .expect("folds");
        assert_eq!(folded.outcome, Outcome::Faulty { ballot: ballot(4) });
        assert_eq!(folded.copies, [None, Some(0)]);
    }

    #[test]
    fn two_copies_of_one_generation_that_disagree_are_damage() {
        let (bytes, frames) = segment(&[(1, &[(3, 1, 4, 0xA)])]);
        let segments = BTreeMap::from([(0, bytes.clone())]);
        let a = record(&frames[0]);
        let b = SlotRecord {
            ballot: ballot(9),
            ..a
        };
        assert!(
            fold_slot(
                Slot(3),
                [Copy::Persisted(a), Copy::Persisted(b)],
                &seen(&bytes),
                &segments,
                1,
                decode,
            )
            .is_err()
        );
    }

    #[test]
    fn a_damaged_head_costs_one_block_of_the_walk() {
        let (mut bytes, _) = segment(&[(1, &[(3, 1, 4, 0xA)]), (2, &[(4, 1, 4, 0xB)])]);
        bytes[3] ^= 0xFF;
        let scan = scan_segment(0, &bytes, 512);
        assert_eq!(scan.heads.len(), 1, "only the intact head");
        assert_eq!(scan.frames.len(), 1);
        assert_eq!(scan.frames[0].frame.slot, Slot(4));
    }
}
