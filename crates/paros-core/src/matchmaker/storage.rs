//! The matchmaker's **read-only recovery port**: how a boot reads the durable
//! registry back, one record at a time.

use std::collections::BTreeMap;

use super::{MatchmakerHardState, MatchmakerWriteOp, Registration};
use crate::types::{Ballot, JournalKey};

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
    fn registration(&self, journal: JournalKey, ballot: Ballot) -> Option<Registration>;

    /// Every registered `(journal, ballot)` in ascending order — the
    /// registries' identities, the twin of the `first_slot..=last_slot`
    /// walk. Each names a record [`Self::registration`] serves.
    fn registered_ballots(&self) -> Vec<(JournalKey, Ballot)>;
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
    registry: BTreeMap<(JournalKey, Ballot), Registration>,
}

impl MemRegistry {
    /// A registry holding `hard_state` and `registrations` — a boot image.
    #[must_use]
    pub fn new(
        hard_state: MatchmakerHardState,
        registrations: BTreeMap<(JournalKey, Ballot), Registration>,
    ) -> Self {
        Self {
            hard_state,
            registry: registrations,
        }
    }

    /// The durable scalars as they stand.
    #[must_use]
    pub fn hard_state(&self) -> &MatchmakerHardState {
        &self.hard_state
    }

    /// The registration records as they stand, in `(journal, ballot)`
    /// order.
    #[must_use]
    pub fn registrations(&self) -> &BTreeMap<(JournalKey, Ballot), Registration> {
        &self.registry
    }

    /// Drop every record of a journal below its durable watermark.
    fn collect(&mut self) {
        let scalars = &self.hard_state;
        self.registry
            .retain(|(journal, ballot), _| *ballot >= scalars.gc_watermark(*journal));
    }

    /// Apply one staged write, with the semantics the op documents:
    /// [`Register`](MatchmakerWriteOp::Register) appends the record;
    /// [`SetGcWatermark`](MatchmakerWriteOp::SetGcWatermark) raises the
    /// journal's watermark (never lowers it) and drops every record of it
    /// below it; [`SetScalars`](MatchmakerWriteOp::SetScalars) replaces the
    /// scalars, keeping the higher of the two watermarks per journal and
    /// dropping below it; [`InstallRegistry`](MatchmakerWriteOp::InstallRegistry)
    /// replaces both, the records filtered at the installed watermarks.
    pub fn apply(&mut self, op: &MatchmakerWriteOp) {
        match op {
            MatchmakerWriteOp::Register {
                journal,
                ballot,
                registration,
            } => {
                self.registry
                    .insert((*journal, *ballot), registration.clone());
            }
            MatchmakerWriteOp::SetGcWatermark { journal, watermark } => {
                let scalars = self.hard_state.registries.entry(*journal).or_default();
                if *watermark > scalars.gc_watermark {
                    scalars.gc_watermark = *watermark;
                    self.collect();
                }
            }
            MatchmakerWriteOp::SetScalars(scalars) => {
                let held = std::mem::replace(&mut self.hard_state, scalars.clone());
                for (journal, kept) in held.registries {
                    let now = self.hard_state.registries.entry(journal).or_default();
                    now.gc_watermark = now.gc_watermark.max(kept.gc_watermark);
                }
                self.collect();
            }
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => {
                self.hard_state = scalars.clone();
                self.registry = registrations
                    .iter()
                    .flat_map(|(journal, records)| {
                        records
                            .iter()
                            .map(move |(ballot, r)| ((*journal, *ballot), r.clone()))
                    })
                    .collect();
                self.collect();
            }
        }
    }
}

impl RegistryStorage for MemRegistry {
    fn initial_state(&self) -> MatchmakerHardState {
        self.hard_state.clone()
    }

    fn registration(&self, journal: JournalKey, ballot: Ballot) -> Option<Registration> {
        self.registry.get(&(journal, ballot)).cloned()
    }

    fn registered_ballots(&self) -> Vec<(JournalKey, Ballot)> {
        self.registry.keys().copied().collect()
    }
}
