//! The storage error types as a caller sees them: the record a fault names,
//! the text an operator reads in a log, and the error chain of a driver's
//! exit.

use std::error::Error as _;

use moonpool_core::SimulationError;
use paros_core::journal::{Command, Control, Entry, LeaderUuid, Seq};
use paros_core::{Ballot, NodeId, Slot, Value};
use paros_model::corruption::{CorruptionVerdict, IntegrityFault};

use crate::{
    BootRefusal, MetadataFault, RunError, StorageError, StorageRecord, WriteOutcome, command_bytes,
};

/// Each record names itself, with its slot or ballot when it has one.
#[test]
fn each_record_has_its_own_name() {
    let names = [
        (StorageRecord::Promise, "promise"),
        (StorageRecord::Accepted(Slot(7)), "accepted[7]"),
        (StorageRecord::ChosenIndex, "chosen-index"),
        (StorageRecord::Truncation, "truncation"),
        (StorageRecord::Batch, "batch"),
        (
            StorageRecord::Registration(Ballot {
                round: 3,
                node: NodeId(2),
            }),
            "registration[3.2]",
        ),
        (StorageRecord::MatchmakerScalars, "matchmaker-scalars"),
        (StorageRecord::Store, "store"),
    ];
    for (record, name) in names {
        assert_eq!(record.to_string(), name);
    }
}

/// Each error names its fault kind, its record and its outcome; a metadata
/// fault names the store.
#[test]
fn each_error_names_its_kind_record_and_outcome() {
    let cases = [
        (
            StorageError::Io {
                record: StorageRecord::Promise,
                outcome: WriteOutcome::Lost,
            },
            StorageRecord::Promise,
            "storage io error on promise (lost)",
        ),
        (
            StorageError::FsyncFailed {
                record: StorageRecord::Batch,
                outcome: WriteOutcome::Unknown,
            },
            StorageRecord::Batch,
            "storage fsync failed on batch (outcome unknown)",
        ),
        (
            StorageError::Corruption {
                record: StorageRecord::Accepted(Slot(4)),
                fault: IntegrityFault::ChecksumMismatch,
                verdict: CorruptionVerdict::Corrupted,
            },
            StorageRecord::Accepted(Slot(4)),
            "storage corruption on accepted[4]: checksum mismatch (corruption)",
        ),
        (
            StorageError::Full {
                record: StorageRecord::Truncation,
            },
            StorageRecord::Truncation,
            "storage full on truncation (lost)",
        ),
        (
            StorageError::Metadata {
                fault: MetadataFault::Missing,
            },
            StorageRecord::Store,
            "storage metadata fault: store missing",
        ),
        (
            StorageError::Metadata {
                fault: MetadataFault::WrongSize,
            },
            StorageRecord::Store,
            "storage metadata fault: store has wrong size",
        ),
        (
            StorageError::Metadata {
                fault: MetadataFault::ReadOnly,
            },
            StorageRecord::Store,
            "storage metadata fault: store is read-only",
        ),
    ];
    for (error, record, text) in cases {
        assert_eq!(error.record(), record, "{error:?}");
        assert_eq!(error.to_string(), text);
    }
}

/// A write command counts its records' bytes, at least one; a control
/// command counts one.
#[test]
fn a_command_counts_its_payload_bytes_and_at_least_one() {
    let write = |records: Vec<Value>| {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(1),
            records,
        })
    };
    assert_eq!(
        command_bytes(&write(vec![Value(vec![0; 3]), Value(vec![0; 4])])),
        7
    );
    assert_eq!(command_bytes(&write(vec![Value(Vec::new())])), 1);
    assert_eq!(command_bytes(&write(Vec::new())), 1);
    assert_eq!(command_bytes(&Command::Control(Control::Noop)), 1);
}

/// Each boot refusal has a stable label and its own exit text.
#[test]
fn each_boot_refusal_has_a_label_and_an_exit_text() {
    let cases = [
        (BootRefusal::Amnesia, "amnesia", "(amnesia)"),
        (
            BootRefusal::AlreadyFormatted,
            "already_formatted",
            "already formatted",
        ),
        (
            BootRefusal::ConfigMismatch,
            "config_mismatch",
            "another configuration",
        ),
    ];
    for (refusal, label, words) in cases {
        assert_eq!(refusal.label(), label);
        let exit = RunError::Refused(refusal);
        let text = exit.to_string();
        assert!(
            text.starts_with("boot refused: ") && text.contains(words),
            "{text}"
        );
        assert!(exit.source().is_none(), "a refusal has no cause");
    }
}

/// A storage fault and an infrastructure failure keep their cause in the
/// error chain.
#[test]
fn a_driver_exit_keeps_its_cause() {
    let fault = StorageError::Full {
        record: StorageRecord::Batch,
    };
    let exit = RunError::Storage(fault.clone());
    assert_eq!(
        exit.to_string(),
        "storage fault, crashing: storage full on batch (lost)"
    );
    assert_eq!(
        exit.source().map(ToString::to_string),
        Some(fault.to_string())
    );
    let infra = RunError::from(SimulationError::InvalidState("bind".into()));
    assert!(matches!(infra, RunError::Infra(_)));
    let cause = infra.source().map(ToString::to_string).expect("a cause");
    assert_eq!(
        infra.to_string(),
        format!("infrastructure failure: {cause}")
    );
    assert!(cause.contains("bind"), "{cause}");
}
