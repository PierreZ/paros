//! The audit's own pure helpers: the decision a storage fault gets, and the
//! report a journal `Read` answer becomes.

use paros_core::journal::{JournalView, Seq};
use paros_core::replica::{LogPage, LogRead};
use paros_core::{Slot, Value};
use paros_model::corruption::{CorruptionVerdict, IntegrityFault};
use paros_storage::{MetadataFault, StorageError, StorageRecord, WriteOutcome};

use super::{LogReadAnswer, LogReadReport, StorageFaultDecision};

#[test]
fn a_full_disk_is_read_only_and_every_other_fault_crashes() {
    let record = StorageRecord::Accepted(Slot(3));
    assert_eq!(
        StorageFaultDecision::of(&StorageError::Full { record }),
        StorageFaultDecision::ReadOnly
    );
    for error in [
        StorageError::Io {
            record,
            outcome: WriteOutcome::Lost,
        },
        StorageError::FsyncFailed {
            record,
            outcome: WriteOutcome::Unknown,
        },
        StorageError::Corruption {
            record,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        },
        StorageError::Metadata {
            fault: MetadataFault::Missing,
        },
    ] {
        assert_eq!(
            StorageFaultDecision::of(&error),
            StorageFaultDecision::Crash,
            "{error:?}"
        );
    }
}

fn view(next: u64, first: u64) -> JournalView {
    JournalView {
        leader: None,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

#[test]
fn a_page_reports_its_records_and_state() {
    let read = LogRead::Page(LogPage {
        from: Seq(4),
        records: vec![Value(vec![1]), Value(vec![2])],
        state: view(6, 2),
    });
    assert_eq!(
        LogReadReport::of(&read, Seq(4), LogReadAnswer::Woke),
        Some(LogReadReport {
            from: Seq(4),
            truncated: false,
            records: &[Value(vec![1]), Value(vec![2])],
            state: view(6, 2),
            answer: LogReadAnswer::Woke,
        })
    );
}

#[test]
fn a_truncated_read_reports_no_record_and_the_new_start() {
    let read = LogRead::Truncated(view(9, 5));
    assert_eq!(
        LogReadReport::of(&read, Seq(1), LogReadAnswer::Immediate),
        Some(LogReadReport {
            from: Seq(1),
            truncated: true,
            records: &[],
            state: view(9, 5),
            answer: LogReadAnswer::Immediate,
        })
    );
}

#[test]
fn a_read_not_held_here_reports_nothing() {
    assert_eq!(
        LogReadReport::of(&LogRead::NotHeld, Seq(1), LogReadAnswer::Expired),
        None
    );
}
