//! Corruption **detection and classification** (Stage 7, CTRL/CLStore-shaped).
//!
//! This module is the typed half of the durable-record contract documented in
//! `docs/analysis/storage/clstore-record-contract.md` and on
//! [`NodeStorage::boot_scan`](crate::NodeStorage::boot_scan): every persisted
//! record is checksummed, every log entry carries a physically separate,
//! itself-checksummed **identifier** `⟨slot, accepted_ballot, offset, cksum⟩`
//! (the entry's persist record), identity lives inside the checksummed region
//! and is re-derived on every read, and absence is detectable (every slot is
//! formatted with a real, checksummed reserved record, so all-zeros is always
//! *faulty*, never "empty").
//!
//! Given that contract, a storage implementation reduces every read-back to
//! per-record **evidence** — did the entry verify, what state is its
//! identifier in, is there durable evidence past it — and this module turns the
//! evidence into a **total, named, exhaustively-tested decision**: a
//! [`RecoveryCase`] per record (the `TigerBeetle` recovery-table style) and the
//! [`CorruptionVerdict`] Stage 8's crash-relevance logic pattern-matches on.
//! Nothing downstream parses strings or rescans traces; the verdict is data on
//! the [`StorageError`](crate::StorageError) surface.
//!
//! The disentanglement rule is CTRL §3.3.3, with the identifier as the persist
//! witness (update protocol: `write(e_i); write(id_i); fsync()` — two writes,
//! ONE fsync):
//!
//! | Local evidence for entry `e_i`                    | Verdict |
//! |---------------------------------------------------|---------|
//! | `id_i` absent, nothing durable past it            | crash-truncatable tail (never acked — discard locally) |
//! | `id_i` present ∧ durable evidence past it         | corruption (crash; Stage 8 recovers) |
//! | `id_i` present ∧ `e_i` is the last entry          | **undecidable** — proven fundamental (CTRL Thm A.1); treated as corruption |
//! | `id_i` and `e_i` both faulty                      | corruption, record unidentifiable (crash) |
//!
//! plus the `TigerBeetle` hardening on the tail rule: truncate-as-crash only
//! inside the window past a provably certain head, cap the window by the
//! maximum concurrently in-flight accept writes, and abandon truncation
//! entirely (⇒ crash) on any witnessed or valid record *inside* the window (it
//! might be a misdirected read).

use std::fmt;

use paros_core::Slot;

/// The fault family a failed integrity check surfaced — *how* the detector
/// caught the record, carried as data on
/// [`StorageError::Corruption`](crate::StorageError::Corruption) so the
/// simulation can correlate injected fault ↔ surfaced error without string
/// parsing, and so Stage 8 can weigh families differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegrityFault {
    /// The record's bytes failed their checksum (bit flip, latent sector
    /// error, torn write).
    ChecksumMismatch,
    /// The record is absent where its identifier (or the reserved-record
    /// contract) says it must exist — a lost write.
    LostWrite,
    /// The checksum passed but the identity inside the checksummed region
    /// names a different record — a misdirected read or write.
    Misdirected,
    /// The read returned an I/O error (`EIO`). Collapsed into the corruption
    /// channel (CTRL §4.1): an unreadable record is treated exactly as a
    /// checksum mismatch ("zero-fill then mismatch" semantics).
    ReadError,
}

impl fmt::Display for IntegrityFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntegrityFault::ChecksumMismatch => write!(f, "checksum mismatch"),
            IntegrityFault::LostWrite => write!(f, "lost write"),
            IntegrityFault::Misdirected => write!(f, "misdirected record"),
            IntegrityFault::ReadError => write!(f, "read error (EIO)"),
        }
    }
}

/// The crash-vs-corruption **disentanglement verdict** for a detected
/// mismatch: what the local evidence proves about the record's history. This
/// is the value Stage 8's crash-relevance logic consumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorruptionVerdict {
    /// A torn write at the tail: the record's persist witness never reached
    /// disk, so the write was never acknowledged to anyone and is safe to
    /// discard locally. This is the *only* verdict that may drop data.
    CrashTail,
    /// A mismatch on a previously persisted (witnessed) record — possibly
    /// chosen, so it must NOT be discarded. Stage 7 reaction: crash. Stage 8:
    /// recover from peers.
    Corrupted,
    /// The evidence cannot distinguish crash from corruption (the last-entry
    /// case, proven fundamental by CTRL Thm A.1, and the hardening's
    /// abandoned-window cases). Treated as corruption: crash; Stage 8's
    /// distributed commitment determination decides.
    Undecidable,
}

impl fmt::Display for CorruptionVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionVerdict::CrashTail => write!(f, "crash-truncatable tail"),
            CorruptionVerdict::Corrupted => write!(f, "corruption"),
            CorruptionVerdict::Undecidable => write!(f, "undecidable"),
        }
    }
}

/// Read-back state of one entry's **identifier** (its persist witness).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WitnessStatus {
    /// The identifier is durable and verifies.
    Present,
    /// The identifier physically reached disk but fails its own checksum.
    Faulty,
    /// The identifier never reached disk (the slot reads back as its reserved
    /// record, or nothing witnessed).
    Absent,
}

/// The evidence booleans one log entry's read-back reduces to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EntryEvidence {
    /// The entry record failed its integrity check (checksum mismatch, lost
    /// write against the reserved-record contract, misdirected identity, or
    /// `EIO` — all one detection channel).
    pub entry_faulty: bool,
    /// State of the entry's separately-written identifier.
    pub identifier: WitnessStatus,
    /// Durable evidence exists past this slot: a later slot whose identifier
    /// physically reached disk (present or faulty). The identifier is the
    /// persist witness, so it alone counts — a later *unwitnessed* entry,
    /// valid or not, is part of the same torn tail (any subset of a batch can
    /// land before its one fsync) and is deliberately **not** successor
    /// evidence.
    pub successor_present: bool,
}

/// The named, total decision for one record — the `TigerBeetle` recovery-table
/// style: every point of the evidence cube maps to exactly one case, each case
/// carries its verdict, and the case label travels on the tracing event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryCase {
    /// Entry and identifier both verify: nothing to do.
    Intact,
    /// Entry verifies but its identifier is damaged: the witness itself
    /// rotted. The entry cannot be trusted as *acknowledged* (corruption).
    IdentifierFaulty,
    /// Entry verifies, identifier absent, durable evidence past it: a
    /// witnessed record exists beyond an unwitnessed one — head-certainty is
    /// broken (possible misdirected read), abandon truncation.
    UnwitnessedInterior,
    /// Entry verifies, identifier absent, nothing durable past it: the final
    /// write of a torn batch — never acknowledged, safe to discard.
    UnwitnessedTail,
    /// Entry faulty, identifier absent, durable evidence past it: a fault
    /// inside the window with proof of later durability — abandon truncation.
    TornInterior,
    /// Entry faulty, identifier absent, nothing durable past it: CTRL's
    /// crash row — the crash hit before `id_i` reached disk; discard locally.
    CrashTruncatableTail,
    /// Entry faulty, identifier present, durable evidence past it: CTRL's
    /// corruption row — the record was fully persisted and later rotted.
    CorruptionBelowTail,
    /// Entry faulty, identifier present, and this is the last entry: CTRL's
    /// proven-undecidable row (Thm A.1) — not an engineering gap.
    LastEntryAmbiguity,
    /// Entry and identifier both faulty: the record is unidentifiable.
    IdentifierLostWithEntry,
    /// Assigned only by [`classify_log`]: a crash-truncatable tail longer than
    /// the maximum concurrently in-flight accept writes — head-certainty is
    /// broken, abandon truncation.
    TailOverBudget,
}

impl RecoveryCase {
    /// The disentanglement verdict this case carries (`None` for an intact
    /// record).
    #[must_use]
    pub fn verdict(self) -> Option<CorruptionVerdict> {
        match self {
            RecoveryCase::Intact => None,
            RecoveryCase::UnwitnessedTail | RecoveryCase::CrashTruncatableTail => {
                Some(CorruptionVerdict::CrashTail)
            }
            RecoveryCase::IdentifierFaulty
            | RecoveryCase::CorruptionBelowTail
            | RecoveryCase::IdentifierLostWithEntry => Some(CorruptionVerdict::Corrupted),
            RecoveryCase::UnwitnessedInterior
            | RecoveryCase::TornInterior
            | RecoveryCase::LastEntryAmbiguity
            | RecoveryCase::TailOverBudget => Some(CorruptionVerdict::Undecidable),
        }
    }

    /// Short, stable label for the tracing event.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            RecoveryCase::Intact => "intact",
            RecoveryCase::IdentifierFaulty => "identifier_faulty",
            RecoveryCase::UnwitnessedInterior => "unwitnessed_interior",
            RecoveryCase::UnwitnessedTail => "unwitnessed_tail",
            RecoveryCase::TornInterior => "torn_interior",
            RecoveryCase::CrashTruncatableTail => "crash_truncatable_tail",
            RecoveryCase::CorruptionBelowTail => "corruption_below_tail",
            RecoveryCase::LastEntryAmbiguity => "last_entry_ambiguity",
            RecoveryCase::IdentifierLostWithEntry => "identifier_lost_with_entry",
            RecoveryCase::TailOverBudget => "tail_over_budget",
        }
    }
}

/// The pure per-record decision: map one entry's evidence to its named case.
///
/// Total over the whole evidence cube (2 entry states × 3 identifier states ×
/// 2 successor states); the unit test enumerates every point. The one place a
/// classic exhaustive unit test is the right tool — it pins a pure function;
/// the simulation still owns end-to-end correctness.
#[must_use]
#[tracing::instrument(level = "trace", skip_all)]
fn decide(evidence: EntryEvidence) -> RecoveryCase {
    match (evidence.entry_faulty, evidence.identifier) {
        (false, WitnessStatus::Present) => RecoveryCase::Intact,
        (false, WitnessStatus::Faulty) => RecoveryCase::IdentifierFaulty,
        (false, WitnessStatus::Absent) => {
            if evidence.successor_present {
                RecoveryCase::UnwitnessedInterior
            } else {
                RecoveryCase::UnwitnessedTail
            }
        }
        (true, WitnessStatus::Present) => {
            if evidence.successor_present {
                RecoveryCase::CorruptionBelowTail
            } else {
                RecoveryCase::LastEntryAmbiguity
            }
        }
        (true, WitnessStatus::Faulty) => RecoveryCase::IdentifierLostWithEntry,
        (true, WitnessStatus::Absent) => {
            if evidence.successor_present {
                RecoveryCase::TornInterior
            } else {
                RecoveryCase::CrashTruncatableTail
            }
        }
    }
}

/// One retained log record's boot-scan evidence, before successor derivation.
#[derive(Clone, Copy, Debug)]
pub struct SlotRecord {
    /// The slot this record claims (already identity-verified where the
    /// identifier holds; a misdirected record reports `entry_faulty`).
    pub slot: Slot,
    /// The entry record failed its integrity check.
    pub entry_faulty: bool,
    /// State of the record's identifier.
    pub identifier: WitnessStatus,
}

/// Classify a node's retained log at boot: derive each record's successor
/// evidence, run `decide` per record, and apply the batching rule plus the
/// `TigerBeetle` hardening on the resulting crash-truncatable tail.
///
/// `records` must be in ascending slot order (the storage sanity backstop —
/// slot indices in the log are in order and monotonically increasing, on
/// `slot` only, never on the accepted ballot). `max_inflight` caps the number
/// of crash-truncatable records by the maximum concurrently in-flight accept
/// writes; a longer unwitnessed tail breaks head-certainty and is reclassified
/// [`RecoveryCase::TailOverBudget`] (⇒ crash).
///
/// # Panics
///
/// Panics if `records` is not in strictly ascending slot order — a
/// misdirected, out-of-order log must be surfaced as per-record evidence by
/// the caller, never silently reordered here.
#[must_use]
#[tracing::instrument(level = "debug", skip_all, fields(records = records.len(), max_inflight))]
pub fn classify_log(records: &[SlotRecord], max_inflight: usize) -> Vec<(Slot, RecoveryCase)> {
    for pair in records.windows(2) {
        assert!(
            pair[0].slot < pair[1].slot,
            "boot-scan records are in strictly ascending slot order"
        );
    }
    // Successor evidence per position: durable proof past this record — a
    // later identifier that physically reached disk (the persist witness is
    // what proves the batch got past a record; entry bytes alone prove
    // nothing about the fsync). Derived by one reverse walk.
    let mut successor = vec![false; records.len()];
    let mut evidence_past = false;
    for (i, record) in records.iter().enumerate().rev() {
        successor[i] = evidence_past;
        let witnessed_here = !matches!(record.identifier, WitnessStatus::Absent);
        evidence_past = evidence_past || witnessed_here;
    }
    let mut cases: Vec<(Slot, RecoveryCase)> = records
        .iter()
        .zip(&successor)
        .map(|(record, &successor_present)| {
            (
                record.slot,
                decide(EntryEvidence {
                    entry_faulty: record.entry_faulty,
                    identifier: record.identifier,
                    successor_present,
                }),
            )
        })
        .collect();
    // TigerBeetle hardening: the crash-truncatable window is bounded by the
    // maximum concurrently in-flight accept writes. A longer tail cannot have
    // come from one torn batch — head-certainty is broken, abandon truncation.
    let tail_len = cases
        .iter()
        .rev()
        .take_while(|(_, case)| matches!(case.verdict(), Some(CorruptionVerdict::CrashTail)))
        .count();
    if tail_len > max_inflight {
        let start = cases.len() - tail_len;
        for (_, case) in &mut cases[start..] {
            *case = RecoveryCase::TailOverBudget;
        }
    }
    cases
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The full evidence cube, every point named: 2 entry states × 3
    /// identifier states × 2 successor states.
    #[test]
    fn decide_is_total_over_the_evidence_cube() {
        let expectations = [
            // (entry_faulty, identifier, successor) -> case
            (false, WitnessStatus::Present, false, RecoveryCase::Intact),
            (false, WitnessStatus::Present, true, RecoveryCase::Intact),
            (
                false,
                WitnessStatus::Faulty,
                false,
                RecoveryCase::IdentifierFaulty,
            ),
            (
                false,
                WitnessStatus::Faulty,
                true,
                RecoveryCase::IdentifierFaulty,
            ),
            (
                false,
                WitnessStatus::Absent,
                false,
                RecoveryCase::UnwitnessedTail,
            ),
            (
                false,
                WitnessStatus::Absent,
                true,
                RecoveryCase::UnwitnessedInterior,
            ),
            (
                true,
                WitnessStatus::Present,
                false,
                RecoveryCase::LastEntryAmbiguity,
            ),
            (
                true,
                WitnessStatus::Present,
                true,
                RecoveryCase::CorruptionBelowTail,
            ),
            (
                true,
                WitnessStatus::Faulty,
                false,
                RecoveryCase::IdentifierLostWithEntry,
            ),
            (
                true,
                WitnessStatus::Faulty,
                true,
                RecoveryCase::IdentifierLostWithEntry,
            ),
            (
                true,
                WitnessStatus::Absent,
                false,
                RecoveryCase::CrashTruncatableTail,
            ),
            (
                true,
                WitnessStatus::Absent,
                true,
                RecoveryCase::TornInterior,
            ),
        ];
        assert_eq!(expectations.len(), 12, "the cube has 2 x 3 x 2 points");
        for (entry_faulty, identifier, successor_present, expected) in expectations {
            let case = decide(EntryEvidence {
                entry_faulty,
                identifier,
                successor_present,
            });
            assert_eq!(
                case, expected,
                "evidence ({entry_faulty}, {identifier:?}, {successor_present})"
            );
        }
    }

    /// Every case's verdict follows the disentanglement table: only the two
    /// tail cases may discard, everything else crashes.
    #[test]
    fn verdicts_follow_the_disentanglement_table() {
        assert_eq!(RecoveryCase::Intact.verdict(), None);
        assert_eq!(
            RecoveryCase::UnwitnessedTail.verdict(),
            Some(CorruptionVerdict::CrashTail)
        );
        assert_eq!(
            RecoveryCase::CrashTruncatableTail.verdict(),
            Some(CorruptionVerdict::CrashTail)
        );
        assert_eq!(
            RecoveryCase::CorruptionBelowTail.verdict(),
            Some(CorruptionVerdict::Corrupted)
        );
        assert_eq!(
            RecoveryCase::IdentifierFaulty.verdict(),
            Some(CorruptionVerdict::Corrupted)
        );
        assert_eq!(
            RecoveryCase::IdentifierLostWithEntry.verdict(),
            Some(CorruptionVerdict::Corrupted)
        );
        assert_eq!(
            RecoveryCase::LastEntryAmbiguity.verdict(),
            Some(CorruptionVerdict::Undecidable)
        );
        assert_eq!(
            RecoveryCase::UnwitnessedInterior.verdict(),
            Some(CorruptionVerdict::Undecidable)
        );
        assert_eq!(
            RecoveryCase::TornInterior.verdict(),
            Some(CorruptionVerdict::Undecidable)
        );
        assert_eq!(
            RecoveryCase::TailOverBudget.verdict(),
            Some(CorruptionVerdict::Undecidable)
        );
    }

    fn record(slot: u64, entry_faulty: bool, identifier: WitnessStatus) -> SlotRecord {
        SlotRecord {
            slot: Slot(slot),
            entry_faulty,
            identifier,
        }
    }

    #[test]
    fn clean_log_classifies_intact() {
        let cases = classify_log(
            &[
                record(0, false, WitnessStatus::Present),
                record(1, false, WitnessStatus::Present),
            ],
            4,
        );
        assert!(cases.iter().all(|(_, c)| *c == RecoveryCase::Intact));
    }

    /// A torn final batch — entries with lost identifiers at the very end —
    /// is a crash-truncatable tail, valid and faulty members alike.
    #[test]
    fn torn_tail_is_crash_truncatable() {
        let cases = classify_log(
            &[
                record(0, false, WitnessStatus::Present),
                record(1, true, WitnessStatus::Absent),
                record(2, false, WitnessStatus::Absent),
            ],
            4,
        );
        assert_eq!(cases[0].1, RecoveryCase::Intact);
        assert_eq!(cases[1].1, RecoveryCase::CrashTruncatableTail);
        assert_eq!(cases[2].1, RecoveryCase::UnwitnessedTail);
    }

    /// A witnessed record past an unwitnessed one breaks head-certainty: the
    /// unwitnessed record is not a tail and must not be discarded.
    #[test]
    fn witnessed_successor_aborts_the_tail() {
        let cases = classify_log(
            &[
                record(0, true, WitnessStatus::Absent),
                record(1, false, WitnessStatus::Present),
            ],
            4,
        );
        assert_eq!(cases[0].1, RecoveryCase::TornInterior);
        assert_eq!(cases[1].1, RecoveryCase::Intact);
    }

    /// A faulty record below a witnessed successor is corruption, and a
    /// faulty *last* record with its identifier present is the proven
    /// undecidable case.
    #[test]
    fn corruption_below_tail_and_last_entry_ambiguity() {
        let cases = classify_log(
            &[
                record(3, true, WitnessStatus::Present),
                record(4, true, WitnessStatus::Present),
            ],
            4,
        );
        assert_eq!(cases[0].1, RecoveryCase::CorruptionBelowTail);
        assert_eq!(cases[1].1, RecoveryCase::LastEntryAmbiguity);
    }

    /// A crash-truncatable tail longer than the in-flight bound cannot have
    /// come from one torn batch: reclassified, truncation abandoned.
    #[test]
    fn overlong_tail_breaks_head_certainty() {
        let records: Vec<SlotRecord> = (0..3)
            .map(|slot| record(slot, true, WitnessStatus::Absent))
            .collect();
        let cases = classify_log(&records, 2);
        assert!(
            cases
                .iter()
                .all(|(_, c)| *c == RecoveryCase::TailOverBudget)
        );
        let cases = classify_log(&records, 3);
        assert!(
            cases
                .iter()
                .all(|(_, c)| *c == RecoveryCase::CrashTruncatableTail)
        );
    }

    #[test]
    #[should_panic(expected = "ascending slot order")]
    fn out_of_order_records_are_rejected() {
        let _ = classify_log(
            &[
                record(2, false, WitnessStatus::Present),
                record(1, false, WitnessStatus::Present),
            ],
            4,
        );
    }
}
