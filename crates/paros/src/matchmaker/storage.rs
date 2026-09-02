//! The matchmaker's durable seam: the [`MatchmakerStorage`] write extension the
//! driver persists the registry through, over the core's read-only
//! [`RegistryStorage`] recovery port, and the default in-memory
//! [`MemMatchmakerStorage`] implementing both.
//!
//! The split is the node's, mirrored on purpose: [`RegistryStorage`] is to the
//! matchmaker what [`paros_core::Storage`] is to the node — the core reads its
//! durable state back through it once, at construction, record by record — and
//! [`MatchmakerStorage`] is the [`NodeStorage`](crate::NodeStorage) twin: the
//! driver owns every write, applies each
//! [`MatchmakerWriteOp`](paros_core::MatchmakerWriteOp) through the matching
//! method here, then [`sync`](MatchmakerStorage::sync)s the batch **before** its
//! reply leaves (persist-before-reply).
//!
//! # Why the registry gets a real storage interface
//!
//! Because it is durable state that will rot, and the recovery story built for
//! the accepted log (CTRL, Stages 7–8) is only available to state that crosses a
//! seam like this one. Every registration is one checksummed record whose
//! identity — the ballot — sits inside the checksummed region, so
//! [`boot_scan`](MatchmakerStorage::boot_scan) can verify and classify each
//! record before any byte reaches the core: a torn tail is discardable (never
//! acknowledged: the reply only leaves after the fsync), a record whose bytes
//! are lost but whose ballot survived is *recoverable* (the other matchmakers
//! hold the same bytes), and only a record whose identity is also lost is a
//! crash. That tri-state — report the ballot as faulty, never as "no
//! configuration here" — is the registry's version of CTRL's central rule,
//! and it lands as a defaulted `faulty_registrations()` on [`RegistryStorage`]
//! beside the per-record read, exactly as [`Storage::faulty_entries`](paros_core::Storage::faulty_entries)
//! landed for the log. A registry booted from one blob could offer none of
//! this: one checksum, one verdict, and a matchmaker that either boots blind or
//! not at all. Every write returns [`Result`] for the same reason: the faults
//! are injectable from the start, through the existing durable-record contract
//! (`docs/analysis/storage/clstore-record-contract.md`) — there is no
//! matchmaker-specific disk-fault story, only the generic one applied to one
//! more record family.

use paros_core::{AcceptorConfig, Ballot, MatchmakerHardState, RegistryStorage};
use std::collections::BTreeMap;

use crate::storage::StorageError;

/// The write side of matchmaker storage: **semantic per-record ops**, the
/// [`NodeStorage`](crate::NodeStorage) twin over the [`RegistryStorage`] port.
pub trait MatchmakerStorage: RegistryStorage {
    /// Boot-time integrity scan, run once per incarnation **before** the core
    /// reads the store (the [`NodeStorage::boot_scan`](crate::NodeStorage::boot_scan)
    /// twin): verify every registration record and the watermark scalar,
    /// classify every mismatch, discard only a crash-truncatable tail (a
    /// registration is acknowledged only after its fsync, so an un-synced
    /// tail was never promised to anyone), and surface the first crash
    /// verdict. **Never truncate on a corruption verdict.** The default
    /// reports a clean store, for storage that cannot rot.
    ///
    /// # Errors
    /// Returns the first classified [`StorageError`] whose verdict requires a
    /// crash.
    fn boot_scan(&mut self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Persist the registration of `config` under `ballot` as one record.
    /// Append-only: the core only ever registers strictly above the highest
    /// ballot it holds, so this is never an overwrite.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn register(&mut self, ballot: Ballot, config: &AcceptorConfig) -> Result<(), StorageError>;

    /// Persist a raised GC watermark and drop every registration record below
    /// it. Monotone: a store never lowers its watermark.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError>;

    /// Flush this batch's writes to stable storage. Every matchmaker write is
    /// safety-critical, so this is always an fsync: the batch must be durable
    /// on return, because the reply that follows claims it is.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the flush fails.
    fn sync(&mut self) -> Result<(), StorageError>;
}

/// The library's default in-memory matchmaker storage: the scalars and the
/// per-ballot registration records stored separately (never a single blob).
#[derive(Clone, Debug, Default)]
pub struct MemMatchmakerStorage {
    hard_state: MatchmakerHardState,
    registry: BTreeMap<Ballot, AcceptorConfig>,
}

impl MemMatchmakerStorage {
    /// A fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RegistryStorage for MemMatchmakerStorage {
    fn initial_state(&self) -> MatchmakerHardState {
        self.hard_state.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<AcceptorConfig> {
        self.registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.registry.keys().copied().collect()
    }
}

impl MatchmakerStorage for MemMatchmakerStorage {
    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    fn register(&mut self, ballot: Ballot, config: &AcceptorConfig) -> Result<(), StorageError> {
        self.registry.insert(ballot, config.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        if watermark > self.hard_state.gc_watermark {
            self.hard_state.gc_watermark = watermark;
            self.registry = self.registry.split_off(&watermark);
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn sync(&mut self) -> Result<(), StorageError> {
        // In-memory: writes are already visible; nothing to flush.
        Ok(())
    }
}

/// The behavioral **contract suite** every [`MatchmakerStorage`] implementation
/// must pass, run against [`MemMatchmakerStorage`] here and against the
/// simulation's world-backed store in `paros-sim`, so a fake can never drift
/// from the trait contract. `fresh` returns an empty store; `reopen` simulates
/// a clean reboot of the same store — every read-back goes through it and
/// through the [`RegistryStorage`] port, because that is how the core reads
/// durable state: once, at construction, record by record.
///
/// # Panics
///
/// Panics on any contract violation.
#[doc(hidden)]
#[tracing::instrument(level = "debug", skip_all)]
pub fn matchmaker_storage_contract_suite<S: MatchmakerStorage>(
    mut fresh: impl FnMut() -> S,
    mut reopen: impl FnMut(S) -> S,
) {
    use paros_core::{Matchmaker, MatchmakerId, NodeId, QuorumSystem};
    let ballot = |round: u64| Ballot {
        round,
        node: NodeId(1),
    };
    // What every reopen must satisfy: the durable records and the durable
    // scalars are mutually consistent (no record below the watermark, every
    // walked ballot readable), and — the recovery contract itself — a
    // matchmaker booted from the port holds exactly what the port serves.
    let consistent = |s: &S| {
        let state = s.initial_state();
        let ballots = s.registered_ballots();
        assert!(
            ballots.windows(2).all(|w| w[0] < w[1]),
            "registered ballots are strictly ascending"
        );
        assert!(
            ballots.iter().all(|b| *b >= state.gc_watermark),
            "no registration survives below the durable watermark"
        );
        let booted = Matchmaker::new(MatchmakerId(0), s);
        assert_eq!(
            *booted.hard_state(),
            state,
            "a boot adopts the durable scalars"
        );
        assert_eq!(
            booted.registry().keys().copied().collect::<Vec<_>>(),
            ballots,
            "a boot walks back every durable registration"
        );
        for ballot in &ballots {
            assert_eq!(
                booted.registry().get(ballot).cloned(),
                s.registration(*ballot),
                "a boot reads each registration back byte for byte"
            );
        }
    };
    let config = |n: u64| {
        AcceptorConfig::new(
            (0..n).map(NodeId).collect::<Vec<_>>(),
            QuorumSystem::Majority,
        )
    };

    // A fresh store is empty, and registrations round-trip through a sync as
    // individually readable records.
    let s = fresh();
    assert_eq!(
        s.initial_state(),
        MatchmakerHardState::default(),
        "a fresh store holds the zero watermark"
    );
    assert!(
        s.registered_ballots().is_empty(),
        "a fresh store holds no registration"
    );
    consistent(&s);
    let mut s = reopen(s);
    s.register(ballot(1), &config(3)).expect("register 1");
    s.register(ballot(2), &config(4)).expect("register 2");
    s.sync().expect("sync");
    let mut s = reopen(s);
    consistent(&s);
    assert_eq!(
        s.registered_ballots(),
        vec![ballot(1), ballot(2)],
        "registered ballots read back in ballot order"
    );
    assert_eq!(s.registration(ballot(1)), Some(config(3)));
    assert_eq!(s.registration(ballot(2)), Some(config(4)));
    assert_eq!(
        s.registration(ballot(3)),
        None,
        "an unregistered ballot has no record"
    );
    assert_eq!(s.initial_state().gc_watermark, Ballot::zero());

    // A raised watermark is durable and drops the collected records.
    s.register(ballot(3), &config(5)).expect("register 3");
    s.set_gc_watermark(ballot(2)).expect("raise");
    s.sync().expect("sync raise");
    let mut s = reopen(s);
    consistent(&s);
    assert_eq!(
        s.initial_state().gc_watermark,
        ballot(2),
        "the watermark round-trips"
    );
    assert_eq!(
        s.registered_ballots(),
        vec![ballot(2), ballot(3)],
        "registrations below the watermark are dropped"
    );
    assert_eq!(
        s.registration(ballot(1)),
        None,
        "a collected record is unreadable"
    );

    // The watermark never lowers.
    s.set_gc_watermark(ballot(1)).expect("re-raise lower");
    s.sync().expect("sync no-op");
    let s = reopen(s);
    consistent(&s);
    assert_eq!(
        s.initial_state().gc_watermark,
        ballot(2),
        "the watermark is monotone"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_matchmaker_storage_passes_the_contract_suite() {
        // In-memory writes are immediately visible: a reboot is the same
        // handle.
        matchmaker_storage_contract_suite(MemMatchmakerStorage::new, |s| s);
    }
}
