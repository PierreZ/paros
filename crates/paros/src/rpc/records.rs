//! The journal edge's framing of a user slot (#185): the records of one
//! `Append`, in order, as the opaque [`paros_core::Value`] the core decides.
//!
//! The core never reads these bytes; only the edge frames them on `Append`
//! and unframes them on `Read`. The frame is the generated `RecordBatch`
//! message, so it is stable and self-describing.

use prost::Message as _;

use super::RecordBatch;

/// Frame `records` as the payload of one user slot.
#[must_use]
pub fn encode_records(records: &[Vec<u8>]) -> Vec<u8> {
    RecordBatch {
        records: records.to_vec(),
    }
    .encode_to_vec()
}

/// Unframe a user slot's payload into its records. A payload that is not a
/// record batch (one no `Append` produced) reads back as a single record
/// holding the raw bytes, so a reader is never refused an entry.
#[must_use]
pub fn decode_records(payload: &[u8]) -> Vec<Vec<u8>> {
    RecordBatch::decode(payload).map_or_else(|_| vec![payload.to_vec()], |batch| batch.records)
}

#[cfg(test)]
mod tests {
    use super::{decode_records, encode_records};

    #[test]
    fn records_round_trip_through_one_slot() {
        let records = vec![b"a".to_vec(), Vec::new(), b"ccc".to_vec()];
        assert_eq!(decode_records(&encode_records(&records)), records);
        assert_eq!(decode_records(&encode_records(&[])), Vec::<Vec<u8>>::new());
    }
}
