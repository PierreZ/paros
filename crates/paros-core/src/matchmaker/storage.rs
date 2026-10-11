//! The matchmaker's **read-only recovery port**: how a boot reads the durable
//! registry back, one record at a time.

use std::collections::BTreeMap;

use super::{MatchmakerHardState, MatchmakerWriteOp, Registration};
use crate::types::{Ballot, JournalId};

/// The read-only recovery port of a matchmaker — the registry's
/// [`crate::Storage`], mirrored method for method. The **application**
/// implements it and owns *all* writes; the core only ever *reads back*, once
/// at construction, what the driver has already persisted through its write
/// extension (`paros::MatchmakerStorage`, the `LogStorage` twin).
///
/// # Why a per-record port and not a state blob
///
/// A matchmaker could be booted from one `(registry map, watermark)` value —
/// the registry is small. It is not, on purpose, because the registry is
/// durable state that **will rot**, and the CTRL recovery story built for the
/// accepted log (Stages 7–8: `docs/analysis/storage/ctrl-multipaxos-restatement.md`)
/// only applies to state the core reads *record by record* through a port the
/// storage layer can classify at its seam:
///
/// - **Detection lives in the write layer's `boot_scan`, per record.** Each
///   registration is one checksummed record with its identity — the ballot —
///   in the checksummed region, so a torn, misdirected or bit-flipped
///   registration is classified *before* any byte reaches this port, exactly
///   as an accepted entry is. A blob would have one checksum for the whole
///   registry and one verdict: crash.
/// - **The tri-state lands here.** CTRL's insight for the log — a record whose
///   *value* is lost but whose *identity* survived must be reported as
///   `faulty`, never as "nothing here" — holds for the registry with the same
///   force: a lost registration answered as "no configuration below `b`"
///   under-reports a history, which is precisely the bug class matchmakers
///   exist to prevent. The repair is not a local one: a matchmaker whose
///   durable state is unusable is **replaced** through a matchmaker-set
///   reconfiguration (the module doc's *Generations*), reconstructed from the
///   surviving quorum — never repaired in place.
/// - **Per-record writes are what make the seams honest.** The driver applies
///   one [`MatchmakerWriteOp`](crate::MatchmakerWriteOp) per record and fsyncs the batch before the
///   reply leaves; a boot that reads records back one by one is the read-side
///   pair of that write ordering, and the audit compares the two.
///
/// Bootstrap and restart are the same path: a fresh matchmaker is an empty
/// port. All methods are infallible: a record that fails its integrity check
/// never reaches the core (the scan withholds it, and crashes or classifies).
pub trait RegistryStorage {
    /// The durable scalars to initialize the matchmaker with. Called once, at
    /// construction.
    fn initial_state(&self) -> MatchmakerHardState;

    /// The record registered under `ballot` in `journal`'s registry, if any
    /// — the per-record read, the twin of [`crate::Storage::accepted`].
    fn registration(&self, journal: JournalId, ballot: Ballot) -> Option<Registration>;

    /// Every registered `(journal, ballot)` in ascending order — the
    /// registries' identities, the twin of the `first_slot..=last_slot`
    /// walk (#190: one registry per journal of the set). Each names a
    /// record [`Self::registration`] serves.
    fn registered(&self) -> Vec<(JournalId, Ballot)>;
}

/// The reference in-memory registry: the durable scalars and the per-ballot
/// registration records, stored separately (never one blob), with the
/// library's semantics for each [`MatchmakerWriteOp`] — what a driver's
/// storage must do, written once so tests, model checkers and examples reboot
/// a [`Matchmaker`](super::Matchmaker) from the writes it actually staged
/// rather than from a snapshot of its live state.
///
/// It is the read port ([`RegistryStorage`]) plus [`MemRegistry::apply`], the
/// write side. It is *not* a storage engine: nothing rots, nothing tears, and
/// [`MemRegistry::apply`] never fails. The `paros` crate's
/// `MemMatchmakerStorage` mirrors it method for method behind the driver's
/// fallible write extension.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemRegistry {
    hard_state: MatchmakerHardState,
    registry: BTreeMap<JournalId, BTreeMap<Ballot, Registration>>,
}

impl MemRegistry {
    /// A registry holding `hard_state` and `registrations` — a boot image.
    #[must_use]
    pub fn new(
        hard_state: MatchmakerHardState,
        registrations: BTreeMap<JournalId, BTreeMap<Ballot, Registration>>,
    ) -> Self {
        let mut registry = Self {
            hard_state,
            registry: registrations,
        };
        registry.registry.retain(|_, ledger| !ledger.is_empty());
        registry
    }

    /// The durable scalars as they stand.
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        &self.hard_state
    }

    /// The registration records as they stand, per journal in ballot
    /// order (a journal with no record is absent).
    #[must_use]
    pub fn registrations(&self) -> &BTreeMap<JournalId, BTreeMap<Ballot, Registration>> {
        &self.registry
    }

    /// Drop `journal`'s records below `watermark`.
    fn collect(&mut self, journal: JournalId, watermark: Ballot) {
        if let Some(ledger) = self.registry.get_mut(&journal) {
            *ledger = ledger.split_off(&watermark);
            if ledger.is_empty() {
                self.registry.remove(&journal);
            }
        }
    }

    /// Apply one staged write, with the semantics the op documents:
    /// [`Register`](MatchmakerWriteOp::Register) appends the record to its
    /// journal's registry;
    /// [`SetGcWatermark`](MatchmakerWriteOp::SetGcWatermark) raises the
    /// journal's watermark (never lowers it) and drops every record of that
    /// journal below it;
    /// [`SetScalars`](MatchmakerWriteOp::SetScalars) replaces the scalars,
    /// keeping per journal the higher of the two watermarks and dropping
    /// below it;
    /// [`InstallRegistry`](MatchmakerWriteOp::InstallRegistry) replaces both,
    /// each journal's records filtered at its installed watermark.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn apply(&mut self, op: &MatchmakerWriteOp) {
        match op {
            MatchmakerWriteOp::Register {
                journal,
                ballot,
                registration,
            } => {
                // The persist half of the pair `Matchmaker::new` reads back:
                // a registration lands at or above its journal's floor.
                assert!(
                    *ballot >= self.hard_state.gc_watermark(*journal),
                    "a registration is persisted at or above the watermark"
                );
                self.registry
                    .entry(*journal)
                    .or_default()
                    .insert(*ballot, registration.clone());
            }
            MatchmakerWriteOp::SetGcWatermark { journal, watermark } => {
                if *watermark > self.hard_state.gc_watermark(*journal) {
                    self.hard_state.journal_mut(*journal).gc_watermark = *watermark;
                    self.collect(*journal, *watermark);
                }
                assert!(
                    self.hard_state.gc_watermark(*journal) >= *watermark,
                    "a persisted watermark covers the raise"
                );
            }
            MatchmakerWriteOp::SetScalars(scalars) => {
                let held = std::mem::replace(&mut self.hard_state, scalars.clone());
                // Every journal keeps the higher of the two watermarks.
                for (journal, kept) in held.journals {
                    let watermark = self.hard_state.gc_watermark(journal).max(kept.gc_watermark);
                    self.hard_state.journal_mut(journal).gc_watermark = watermark;
                }
                let floors: Vec<(JournalId, Ballot)> = self
                    .hard_state
                    .journals
                    .iter()
                    .map(|(journal, scalars)| (*journal, scalars.gc_watermark))
                    .collect();
                for (journal, watermark) in floors {
                    self.collect(journal, watermark);
                }
            }
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => {
                self.hard_state = scalars.clone();
                self.registry = registrations
                    .iter()
                    .map(|(journal, ledger)| {
                        let watermark = scalars.gc_watermark(*journal);
                        let kept: BTreeMap<Ballot, Registration> = ledger
                            .range(watermark..)
                            .map(|(b, r)| (*b, r.clone()))
                            .collect();
                        (*journal, kept)
                    })
                    .filter(|(_, ledger)| !ledger.is_empty())
                    .collect();
            }
        }
        // Whatever the op, the store holds nothing below a journal's floor.
        assert!(
            self.registry.iter().all(|(journal, ledger)| ledger
                .keys()
                .next()
                .is_none_or(|b| *b >= self.hard_state.gc_watermark(*journal))),
            "a persisted registry holds nothing below its watermark"
        );
    }
}

impl RegistryStorage for MemRegistry {
    fn initial_state(&self) -> MatchmakerHardState {
        self.hard_state.clone()
    }

    fn registration(&self, journal: JournalId, ballot: Ballot) -> Option<Registration> {
        self.registry
            .get(&journal)
            .and_then(|ledger| ledger.get(&ballot))
            .cloned()
    }

    fn registered(&self) -> Vec<(JournalId, Ballot)> {
        self.registry
            .iter()
            .flat_map(|(journal, ledger)| ledger.keys().map(|ballot| (*journal, *ballot)))
            .collect()
    }
}
