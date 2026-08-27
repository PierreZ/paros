//! Corruption detection: the classified verdict taxonomy and the
//! crash-vs-corruption disentanglement classifier (Stage 7, issue #20).
//!
//! The durable-record contract this classifier assumes (`CLStore` §3.3/§4.1 with
//! `TigerBeetle`'s refinements; see `docs/analysis/storage/record-contract.md`)
//! is: every persisted record is checksummed; every log entry has an
//! *identifier* record `⟨slot, accepted_ballot, offset, cksum⟩` physically
//! separate from the entry and itself checksummed; every slot is formatted with
//! a real, checksummed reserved record so absence is detectable; and identity
//! lives inside the checksummed region and is re-derived on every read. The
//! update protocol per entry is `write(entry); write(identifier); fsync()` —
//! two writes, ONE fsync — which makes the identifier the entry's **persist
//! witness**: an identifier that never reached the disk proves the fsync (and
//! therefore any acknowledgement predicated on it) cannot have completed.
//!
//! The classifier here is a **total, named, exhaustively-tested decision
//! function** over that read-back evidence (the `TigerBeetle` recovery-table
//! style): [`decide`] maps one record's evidence onto a [`RecoveryCase`], and
//! [`classify_log`] applies the batching rule plus the hardening rules across
//! the whole log. It is pure data-in/data-out — no I/O, no clocks — so a unit
//! test enumerates the full evidence cube and the simulation feeds it the
//! semantic read outcomes its storage world models.

use std::fmt;

use paros_core::Slot;

/// How a record's bytes read back, after checksum + identity verification.
///
/// The checksum is validated **before** any other field is touched; identity
/// (the slot/cluster stamped inside the checksummed region) is re-derived on
/// every read, so a record that passes its checksum but answers for the wrong
/// slot is its own outcome ([`WrongIdentity`](RecordState::WrongIdentity)) —
/// a misdirected read or write, not a valid record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordState {
    /// Checksum valid and the identity matches this slot.
    Valid,
    /// The checksum failed. Covers a bit flip, a latent sector error, a torn
    /// write, and an `EIO` read degraded to zero-fill-then-mismatch.
    Mismatch,
    /// The checksum passed but the identity names a different slot (or a
    /// different cluster/file incarnation): a misdirected read or write.
    WrongIdentity,
    /// The slot holds its reserved record: nothing was ever written here.
    /// Reserved records are real, checksummed, slot-stamped values — all-zeros
    /// is *always* invalid — so absence is a verified fact, never a guess.
    Absent,
}

/// How an entry's *identifier* record reads back.
///
/// The identifier is written after its entry and is atomically writable, so its
/// three states carry crash-forensic meaning: `Absent` (reserved) proves the
/// update's fsync never completed for this entry, while `Mismatch` (garbage)
/// means the identifier write itself was torn or the record rotted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentState {
    /// Checksum valid, identity matches.
    Valid,
    /// Checksum failed (torn identifier write, or corruption at rest). A
    /// wrong-identity identifier also classifies here: either way it is not
    /// this slot's witness.
    Mismatch,
    /// The reserved record: the identifier was never written.
    Absent,
}

/// The disentanglement verdict for one mismatched record (CTRL §3.3.3): what
/// the fault *means*, which is what Stage 8's crash-relevance logic consumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorruptionVerdict {
    /// A crash artifact at the log tail: the update protocol proves the record
    /// was never acknowledged to anyone, so discarding it locally is safe.
    /// This is the **only** verdict that ever permits dropping data.
    CrashTail,
    /// Genuine corruption of a previously persisted (possibly chosen) record.
    /// Stage 7 reaction: crash — never truncate (the ZooKeeper/LogCabin
    /// truncate-on-mismatch bug erases committed data cluster-wide). Stage 8
    /// recovers it from peers instead.
    Corrupted,
    /// Proven fundamental ambiguity (CTRL Thm A.1): a mismatched final entry
    /// whose identifier is present cannot be locally told apart from a torn
    /// write racing the fsync. Treated exactly as corruption in Stage 7
    /// (crash); Stage 8's distributed commitment determination decides it.
    Undecidable,
}

/// Which fault family surfaced a detected corruption — carried on the typed
/// error so injected fault ↔ surfaced error correlate without string parsing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorruptionKind {
    /// A checksum mismatch: bit flip, latent sector error, or torn write.
    ChecksumMismatch,
    /// Absence where the identifier proves a record was written (a lost
    /// write), or an identifier lost under a surviving entry.
    LostWrite,
    /// A valid-checksum record answering for the wrong slot: a misdirected
    /// read or write, caught by the identity check.
    Misdirected,
    /// An unreadable record (`EIO` on read), collapsed into the corruption
    /// channel with zero-fill-then-mismatch semantics (CTRL §4.1): one
    /// detection path, one classification path.
    ReadIo,
}

/// A filesystem-metadata fault at *file* granularity (issue #20 item E).
///
/// These are modeled separately from record corruption: metadata faults have
/// no per-record witness to disentangle, so the verdict is **reliably crash**
/// — never attempt recovery on metadata, in Stage 8 either. The oracle for
/// this family is asymmetric: unavailable = pass, unsafe = fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataFault {
    /// The record store is missing or cannot be opened.
    Missing,
    /// The store has the wrong size (fixed-size preallocation plus a
    /// separately stored snapshot size make this checkable).
    WrongSize,
    /// The store is read-only: reads serve, every write fails.
    ReadOnly,
}

impl fmt::Display for MetadataFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataFault::Missing => write!(f, "store missing/unopenable"),
            MetadataFault::WrongSize => write!(f, "store has wrong size"),
            MetadataFault::ReadOnly => write!(f, "store is read-only"),
        }
    }
}

/// The named recovery case for one log record's read-back evidence — the
/// `TigerBeetle` recovery-table style: every point of the evidence cube maps to
/// exactly one named case, the case label travels on the tracing event, and
/// each named case is a coverage anchor proving the sweep reaches the
/// ambiguous shapes, not just the happy path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryCase {
    /// Entry and identifier both valid: a healthy record.
    Clean,
    /// Entry and identifier both reserved: the slot was never written.
    Reserved,
    /// Entry valid, identifier faulty, nothing after it: the crash landed
    /// during `write(identifier)`, after the entry — the fsync cannot have
    /// completed, so nothing was acknowledged. Discard is safe.
    TornIdentifierTail,
    /// Entry valid, identifier faulty, with a written successor: an interior
    /// identifier rotted or was misdirected over. The entry itself is intact,
    /// but its persist witness is gone — corruption, not a crash artifact.
    IdentifierCorrupt,
    /// Entry valid, identifier reserved, nothing after it: the crash landed
    /// between `write(entry)` and `write(identifier)`. Discard is safe.
    UnwitnessedTail,
    /// Entry valid, identifier reserved, with a written successor: an interior
    /// identifier write was lost. The fsync that covered the successor proves
    /// this update's window closed long ago — corruption.
    IdentifierLost,
    /// Entry mismatched, identifier valid, nothing after it: the proven-
    /// fundamental last-entry ambiguity (CTRL Thm A.1) — a torn write racing
    /// the fsync and post-fsync rot are locally indistinguishable.
    AmbiguousTail,
    /// Entry mismatched, identifier valid, with a written successor: a
    /// previously persisted record no longer matches its checksum —
    /// corruption of possibly chosen data.
    CorruptEntry,
    /// Entry mismatched, identifier reserved, nothing after it: the classic
    /// torn tail — the crash landed inside `write(entry)`. Discard is safe.
    TornEntryTail,
    /// Entry mismatched, identifier reserved, with a written successor: the
    /// batching rule's window opener — the first faulty entry *without* an
    /// identifier starts the crash-truncatable suffix, and [`classify_log`]'s
    /// hardening decides whether the suffix is genuinely discardable.
    TornEntryInterior,
    /// The entry passed its checksum but answers for a different slot: a
    /// misdirected read or write, caught by the identity check. Never a crash
    /// artifact (a torn write cannot produce a valid foreign checksum).
    MisdirectedEntry,
    /// Entry reserved, identifier valid, nothing after it: the entry write
    /// was lost but its witness survived — the fsync may have completed and
    /// an acknowledgement may exist, so discard is forbidden; ambiguous.
    LostEntryTail,
    /// Entry reserved, identifier valid, with a written successor: a lost
    /// entry write below the tail — corruption of possibly chosen data.
    LostEntry,
    /// Neither the entry nor the identifier reads back usable (both faulty,
    /// or an identifier torn with no entry ever written): the record is
    /// unidentifiable. A single crash cannot produce this shape (the update
    /// protocol orders the two writes), so it is always corruption.
    UnidentifiableEntry,
}

impl RecoveryCase {
    /// A short, stable label for tracing events and detail maps.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            RecoveryCase::Clean => "clean",
            RecoveryCase::Reserved => "reserved",
            RecoveryCase::TornIdentifierTail => "torn_identifier_tail",
            RecoveryCase::IdentifierCorrupt => "identifier_corrupt",
            RecoveryCase::UnwitnessedTail => "unwitnessed_tail",
            RecoveryCase::IdentifierLost => "identifier_lost",
            RecoveryCase::AmbiguousTail => "ambiguous_tail",
            RecoveryCase::CorruptEntry => "corrupt_entry",
            RecoveryCase::TornEntryTail => "torn_entry_tail",
            RecoveryCase::TornEntryInterior => "torn_entry_interior",
            RecoveryCase::MisdirectedEntry => "misdirected_entry",
            RecoveryCase::LostEntryTail => "lost_entry_tail",
            RecoveryCase::LostEntry => "lost_entry",
            RecoveryCase::UnidentifiableEntry => "unidentifiable_entry",
        }
    }

    /// The disentanglement verdict this case carries (`None` for the two
    /// healthy cases). The mapping is the CTRL §3.3.3 persist-record table:
    /// an absent-or-torn identifier at the tail proves the update's fsync
    /// never completed (discard safe); a present identifier makes the record
    /// possibly acknowledged (never discard); a mismatched final entry under
    /// a valid identifier is the proven-fundamental ambiguity.
    #[must_use]
    pub fn verdict(self) -> Option<CorruptionVerdict> {
        match self {
            RecoveryCase::Clean | RecoveryCase::Reserved => None,
            RecoveryCase::TornIdentifierTail
            | RecoveryCase::UnwitnessedTail
            | RecoveryCase::TornEntryTail
            | RecoveryCase::TornEntryInterior => Some(CorruptionVerdict::CrashTail),
            RecoveryCase::AmbiguousTail | RecoveryCase::LostEntryTail => {
                Some(CorruptionVerdict::Undecidable)
            }
            RecoveryCase::IdentifierCorrupt
            | RecoveryCase::IdentifierLost
            | RecoveryCase::CorruptEntry
            | RecoveryCase::MisdirectedEntry
            | RecoveryCase::LostEntry
            | RecoveryCase::UnidentifiableEntry => Some(CorruptionVerdict::Corrupted),
        }
    }

    /// The fault family the case's evidence points at (`None` for the healthy
    /// cases). An `EIO` read is degraded to [`RecordState::Mismatch`] *before*
    /// classification, so [`CorruptionKind::ReadIo`] is stamped by the read
    /// layer that saw the errno, never derived here.
    #[must_use]
    pub fn kind(self) -> Option<CorruptionKind> {
        match self {
            RecoveryCase::Clean | RecoveryCase::Reserved => None,
            RecoveryCase::MisdirectedEntry => Some(CorruptionKind::Misdirected),
            RecoveryCase::UnwitnessedTail
            | RecoveryCase::IdentifierLost
            | RecoveryCase::LostEntryTail
            | RecoveryCase::LostEntry => Some(CorruptionKind::LostWrite),
            RecoveryCase::TornIdentifierTail
            | RecoveryCase::IdentifierCorrupt
            | RecoveryCase::AmbiguousTail
            | RecoveryCase::CorruptEntry
            | RecoveryCase::TornEntryTail
            | RecoveryCase::TornEntryInterior
            | RecoveryCase::UnidentifiableEntry => Some(CorruptionKind::ChecksumMismatch),
        }
    }
}

/// The pure per-record decision function: one log record's read-back evidence
/// → its named [`RecoveryCase`]. Total over the full evidence cube
/// (4 entry states × 3 identifier states × successor present or not); the
/// exhaustive unit test below enumerates every point.
///
/// `successor_present` is whether any *written* record (entry or identifier
/// not reserved) exists at a higher slot: presence proves a later fsync
/// closed this record's update window, which is what separates a crash
/// artifact from corruption in every identifier-absent row.
#[must_use]
pub fn decide(entry: RecordState, ident: IdentState, successor_present: bool) -> RecoveryCase {
    match (entry, ident, successor_present) {
        // A misdirected entry is never a crash artifact, whatever the
        // identifier says: a torn write cannot forge a valid foreign checksum.
        (RecordState::WrongIdentity, _, _) => RecoveryCase::MisdirectedEntry,
        (RecordState::Valid, IdentState::Valid, _) => RecoveryCase::Clean,
        (RecordState::Valid, IdentState::Mismatch, false) => RecoveryCase::TornIdentifierTail,
        (RecordState::Valid, IdentState::Mismatch, true) => RecoveryCase::IdentifierCorrupt,
        (RecordState::Valid, IdentState::Absent, false) => RecoveryCase::UnwitnessedTail,
        (RecordState::Valid, IdentState::Absent, true) => RecoveryCase::IdentifierLost,
        (RecordState::Mismatch, IdentState::Valid, false) => RecoveryCase::AmbiguousTail,
        (RecordState::Mismatch, IdentState::Valid, true) => RecoveryCase::CorruptEntry,
        (RecordState::Mismatch, IdentState::Absent, false) => RecoveryCase::TornEntryTail,
        (RecordState::Mismatch, IdentState::Absent, true) => RecoveryCase::TornEntryInterior,
        // The update protocol orders write(entry) before write(identifier), so
        // one crash cannot leave the identifier torn while the entry is
        // mismatched or was never written: always corruption, unidentifiable.
        (RecordState::Mismatch | RecordState::Absent, IdentState::Mismatch, _) => {
            RecoveryCase::UnidentifiableEntry
        }
        (RecordState::Absent, IdentState::Valid, false) => RecoveryCase::LostEntryTail,
        (RecordState::Absent, IdentState::Valid, true) => RecoveryCase::LostEntry,
        (RecordState::Absent, IdentState::Absent, _) => RecoveryCase::Reserved,
    }
}

/// One log record's read-back evidence, for [`classify_log`].
#[derive(Clone, Copy, Debug)]
pub struct LogRecord {
    /// The slot this physical record answers for.
    pub slot: Slot,
    /// The entry's read-back state.
    pub entry: RecordState,
    /// The identifier's read-back state.
    pub ident: IdentState,
}

impl LogRecord {
    fn written(&self) -> bool {
        !(self.entry == RecordState::Absent && self.ident == IdentState::Absent)
    }
}

/// The whole-log classification: what boot-time recovery must do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogVerdict {
    /// Every written record is clean: recover everything.
    Clean,
    /// A crash-truncatable tail: discard the records at `discard` (each with
    /// its named case) and recover the prefix below them. Only reachable when
    /// every hardening rule held — the discard sits past the certain head,
    /// within the in-flight window, with nothing valid or corrupt beyond it.
    DiscardTail {
        /// The discarded slots with their per-record cases, in slot order.
        discard: Vec<(Slot, RecoveryCase)>,
    },
    /// Corruption (or hardening abandoned a truncation): crash. Carries the
    /// first fatal record's evidence — the typed classification Stage 8
    /// pattern-matches on.
    Fatal {
        /// The slot whose record is fatal.
        slot: Slot,
        /// Its named recovery case.
        case: RecoveryCase,
        /// The disentanglement verdict (never [`CorruptionVerdict::CrashTail`]).
        verdict: CorruptionVerdict,
    },
}

/// Classify a whole log's read-back evidence: the CTRL batching rule plus the
/// `TigerBeetle` hardening on the tail rule.
///
/// `records` are the log's physical records in ascending slot order (the
/// slot-monotonicity backstop is the caller's parse-time check — on `slot`
/// only, never on the accepted ballot). `certain_head` is the durable chosen
/// index: every slot at or below it is provably acknowledged, so no discard
/// may ever reach it. `max_inflight` caps the number of discardable records
/// by the maximum concurrently in-flight accept writes.
///
/// Rules, in order:
/// - Any record whose case is `Corrupted`/`Undecidable` is fatal (the first
///   one is reported).
/// - The crash-truncatable window opens at the first `CrashTail`-verdict
///   record and must run to the end of the written log: a *valid* record past
///   it abandons truncation (it might be a misdirected read masking the true
///   shape — `TigerBeetle`'s rule), surfacing as `Fatal` on the window opener.
/// - A window at or below `certain_head`, or wider than `max_inflight`
///   written records, breaks head-certainty: `Fatal` on the window opener.
/// - Otherwise the window is a genuine crash artifact: `DiscardTail`.
///
/// # Panics
/// Panics if `records` is not in strictly ascending slot order — the caller's
/// parse-time slot-monotonicity backstop is a precondition here, and feeding
/// an out-of-order log is a programmer error, never an operating condition.
#[must_use]
pub fn classify_log(
    records: &[LogRecord],
    certain_head: Option<Slot>,
    max_inflight: usize,
) -> LogVerdict {
    // Precondition: ascending physical slot order (the parse-time backstop).
    for pair in records.windows(2) {
        assert!(
            pair[0].slot < pair[1].slot,
            "log records must arrive in strictly ascending slot order"
        );
    }
    let last_written = records.iter().rposition(LogRecord::written);
    let mut window_open: Option<(Slot, RecoveryCase)> = None;
    let mut discard: Vec<(Slot, RecoveryCase)> = Vec::new();
    for (i, record) in records.iter().enumerate() {
        let successor_present = match last_written {
            Some(last) => i < last,
            None => false,
        };
        let case = decide(record.entry, record.ident, successor_present);
        match case.verdict() {
            None => {
                if case == RecoveryCase::Clean
                    && let Some((opener_slot, opener_case)) = window_open
                {
                    // Hardening: a valid record past the window opener might be
                    // a misdirected read masking the true shape — abandon the
                    // truncation and crash instead.
                    return LogVerdict::Fatal {
                        slot: opener_slot,
                        case: opener_case,
                        verdict: CorruptionVerdict::Corrupted,
                    };
                }
            }
            Some(CorruptionVerdict::CrashTail) => {
                if window_open.is_none() {
                    window_open = Some((record.slot, case));
                }
                discard.push((record.slot, case));
            }
            Some(verdict) => {
                return LogVerdict::Fatal {
                    slot: record.slot,
                    case,
                    verdict,
                };
            }
        }
    }
    let Some((opener_slot, opener_case)) = window_open else {
        return LogVerdict::Clean;
    };
    // Hardening: the discard must sit strictly past the provably certain head
    // (a chosen slot is provably acknowledged), and the window is capped by
    // the maximum concurrently in-flight accept writes — a wider window means
    // head-certainty itself is broken.
    let head_certain = certain_head.is_none_or(|head| opener_slot > head);
    if !head_certain || discard.len() > max_inflight {
        return LogVerdict::Fatal {
            slot: opener_slot,
            case: opener_case,
            verdict: CorruptionVerdict::Corrupted,
        };
    }
    LogVerdict::DiscardTail { discard }
}

/// Classify the two metainfo (`HardState`) copies (CTRL metainfo doctrine).
///
/// The metainfo is tens of bytes, updated rarely, and kept as **two local
/// checksummed copies**. It is exempt from crash/corruption entanglement
/// entirely (atomic-rename discipline: a partial copy update is discarded on
/// read, so a mismatch is always corruption of that copy): one copy bad ⇒ use
/// the other and repair it; both bad ⇒ crash — the node cannot know what it
/// promised, and no peer can tell it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetainfoVerdict {
    /// Both copies verified.
    Clean,
    /// Exactly one copy failed verification: read the other, rewrite the bad
    /// copy from it. Carries the faulty copy's index (0 or 1).
    RepairCopy(u8),
    /// Both copies failed verification: fatal.
    Fatal,
}

/// Decide the metainfo verdict from the two copies' read-back states.
/// A metainfo copy has no separate identifier (it *is* its own witness), so
/// the evidence is one [`RecordState`] per copy; `WrongIdentity` (a
/// misdirected write clobbering a copy) counts as that copy being bad.
#[must_use]
pub fn decide_metainfo(copy0: RecordState, copy1: RecordState) -> MetainfoVerdict {
    let bad0 = copy0 != RecordState::Valid;
    let bad1 = copy1 != RecordState::Valid;
    match (bad0, bad1) {
        (false, false) => MetainfoVerdict::Clean,
        (true, false) => MetainfoVerdict::RepairCopy(0),
        (false, true) => MetainfoVerdict::RepairCopy(1),
        (true, true) => MetainfoVerdict::Fatal,
    }
}

impl fmt::Display for CorruptionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionKind::ChecksumMismatch => write!(f, "checksum mismatch"),
            CorruptionKind::LostWrite => write!(f, "lost write"),
            CorruptionKind::Misdirected => write!(f, "misdirected write"),
            CorruptionKind::ReadIo => write!(f, "read EIO (zero-fill)"),
        }
    }
}

impl fmt::Display for CorruptionVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorruptionVerdict::CrashTail => write!(f, "crash-truncatable tail"),
            CorruptionVerdict::Corrupted => write!(f, "corrupted"),
            CorruptionVerdict::Undecidable => write!(f, "undecidable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENTRY_STATES: [RecordState; 4] = [
        RecordState::Valid,
        RecordState::Mismatch,
        RecordState::WrongIdentity,
        RecordState::Absent,
    ];
    const IDENT_STATES: [IdentState; 3] =
        [IdentState::Valid, IdentState::Mismatch, IdentState::Absent];

    /// The full evidence cube (4 × 3 × 2 = 24 points), enumerated against the
    /// expected named case — this is the one place a classic exhaustive unit
    /// test is the right tool: it pins a pure function.
    #[test]
    fn decide_is_total_over_the_evidence_cube() {
        use IdentState as I;
        use RecordState as E;
        use RecoveryCase as C;
        let expect = |entry, ident, succ| match (entry, ident, succ) {
            (E::WrongIdentity, _, _) => C::MisdirectedEntry,
            (E::Valid, I::Valid, _) => C::Clean,
            (E::Valid, I::Mismatch, false) => C::TornIdentifierTail,
            (E::Valid, I::Mismatch, true) => C::IdentifierCorrupt,
            (E::Valid, I::Absent, false) => C::UnwitnessedTail,
            (E::Valid, I::Absent, true) => C::IdentifierLost,
            (E::Mismatch, I::Valid, false) => C::AmbiguousTail,
            (E::Mismatch, I::Valid, true) => C::CorruptEntry,
            (E::Mismatch | E::Absent, I::Mismatch, _) => C::UnidentifiableEntry,
            (E::Mismatch, I::Absent, false) => C::TornEntryTail,
            (E::Mismatch, I::Absent, true) => C::TornEntryInterior,
            (E::Absent, I::Valid, false) => C::LostEntryTail,
            (E::Absent, I::Valid, true) => C::LostEntry,
            (E::Absent, I::Absent, _) => C::Reserved,
        };
        let mut points = 0;
        for entry in ENTRY_STATES {
            for ident in IDENT_STATES {
                for succ in [false, true] {
                    let case = decide(entry, ident, succ);
                    assert_eq!(
                        case,
                        expect(entry, ident, succ),
                        "({entry:?}, {ident:?}, succ={succ})"
                    );
                    points += 1;
                }
            }
        }
        assert_eq!(points, 24, "the whole cube was enumerated");
    }

    /// Verdict-space properties over the whole cube: a discard verdict only
    /// ever arises from an identifier that is not a valid witness, an
    /// identifier-present mismatch is never discardable (never truncate on a
    /// mismatch), and every faulty case carries a fault family.
    #[test]
    fn verdicts_respect_the_persist_witness() {
        for entry in ENTRY_STATES {
            for ident in IDENT_STATES {
                for succ in [false, true] {
                    let case = decide(entry, ident, succ);
                    let verdict = case.verdict();
                    // Positive space: the healthy cases carry no verdict and
                    // no kind; every faulty case carries both.
                    assert_eq!(verdict.is_none(), case.kind().is_none());
                    if verdict == Some(CorruptionVerdict::CrashTail) {
                        // Discard is only ever justified by the persist
                        // witness: identifier not valid (fsync unproven) and
                        // no written successor (window still open).
                        assert_ne!(ident, IdentState::Valid, "{case:?}");
                        // The one interior discard case is the batch-window
                        // opener, resolved by classify_log's hardening.
                        if succ {
                            assert_eq!(case, RecoveryCase::TornEntryInterior);
                        }
                    }
                    // Negative space: a valid identifier means possibly
                    // acknowledged — never discardable.
                    if ident == IdentState::Valid {
                        assert_ne!(verdict, Some(CorruptionVerdict::CrashTail), "{case:?}");
                    }
                }
            }
        }
    }

    fn rec(slot: u64, entry: RecordState, ident: IdentState) -> LogRecord {
        LogRecord {
            slot: Slot(slot),
            entry,
            ident,
        }
    }

    #[test]
    fn clean_log_classifies_clean() {
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Valid, IdentState::Valid),
            rec(3, RecordState::Absent, IdentState::Absent),
        ];
        assert_eq!(classify_log(&log, Some(Slot(1)), 4), LogVerdict::Clean);
    }

    #[test]
    fn torn_tail_is_discarded() {
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Absent),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(0)), 4),
            LogVerdict::DiscardTail {
                discard: vec![(Slot(1), RecoveryCase::TornEntryTail)]
            }
        );
    }

    #[test]
    fn multi_record_torn_suffix_is_discarded_within_the_window() {
        // Pipelined accepts: the crash tore the last two updates.
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Absent),
            rec(2, RecordState::Valid, IdentState::Absent),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(0)), 4),
            LogVerdict::DiscardTail {
                discard: vec![
                    (Slot(1), RecoveryCase::TornEntryInterior),
                    (Slot(2), RecoveryCase::UnwitnessedTail),
                ]
            }
        );
    }

    #[test]
    fn valid_record_past_the_window_abandons_truncation() {
        // TigerBeetle hardening: a clean record beyond the window opener might
        // be a misdirected read masking the true shape — crash, don't truncate.
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Absent),
            rec(2, RecordState::Valid, IdentState::Valid),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(0)), 4),
            LogVerdict::Fatal {
                slot: Slot(1),
                case: RecoveryCase::TornEntryInterior,
                verdict: CorruptionVerdict::Corrupted,
            }
        );
    }

    #[test]
    fn corruption_below_the_tail_is_fatal() {
        let log = [
            rec(0, RecordState::Mismatch, IdentState::Valid),
            rec(1, RecordState::Valid, IdentState::Valid),
        ];
        assert_eq!(
            classify_log(&log, None, 4),
            LogVerdict::Fatal {
                slot: Slot(0),
                case: RecoveryCase::CorruptEntry,
                verdict: CorruptionVerdict::Corrupted,
            }
        );
    }

    #[test]
    fn last_entry_ambiguity_is_fatal_undecidable() {
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Valid),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(0)), 4),
            LogVerdict::Fatal {
                slot: Slot(1),
                case: RecoveryCase::AmbiguousTail,
                verdict: CorruptionVerdict::Undecidable,
            }
        );
    }

    #[test]
    fn discard_never_reaches_the_certain_head() {
        // The torn record sits AT the durable chosen index: provably
        // acknowledged, so head-certainty is broken — crash, don't truncate.
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Absent),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(1)), 4),
            LogVerdict::Fatal {
                slot: Slot(1),
                case: RecoveryCase::TornEntryTail,
                verdict: CorruptionVerdict::Corrupted,
            }
        );
    }

    #[test]
    fn discard_window_is_capped_by_in_flight_writes() {
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::Mismatch, IdentState::Absent),
            rec(2, RecordState::Mismatch, IdentState::Absent),
            rec(3, RecordState::Mismatch, IdentState::Absent),
        ];
        assert!(matches!(
            classify_log(&log, Some(Slot(0)), 2),
            LogVerdict::Fatal {
                verdict: CorruptionVerdict::Corrupted,
                ..
            }
        ));
        assert!(matches!(
            classify_log(&log, Some(Slot(0)), 3),
            LogVerdict::DiscardTail { .. }
        ));
    }

    #[test]
    fn misdirected_record_is_fatal_even_in_the_tail_window() {
        let log = [
            rec(0, RecordState::Valid, IdentState::Valid),
            rec(1, RecordState::WrongIdentity, IdentState::Absent),
        ];
        assert_eq!(
            classify_log(&log, Some(Slot(0)), 4),
            LogVerdict::Fatal {
                slot: Slot(1),
                case: RecoveryCase::MisdirectedEntry,
                verdict: CorruptionVerdict::Corrupted,
            }
        );
    }

    #[test]
    fn metainfo_verdicts_cover_both_copies() {
        assert_eq!(
            decide_metainfo(RecordState::Valid, RecordState::Valid),
            MetainfoVerdict::Clean
        );
        assert_eq!(
            decide_metainfo(RecordState::Mismatch, RecordState::Valid),
            MetainfoVerdict::RepairCopy(0)
        );
        assert_eq!(
            decide_metainfo(RecordState::Valid, RecordState::WrongIdentity),
            MetainfoVerdict::RepairCopy(1)
        );
        assert_eq!(
            decide_metainfo(RecordState::Mismatch, RecordState::Mismatch),
            MetainfoVerdict::Fatal
        );
    }
}
