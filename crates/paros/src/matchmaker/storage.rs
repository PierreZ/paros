//! The matchmaker's durable seam: the [`MatchmakerStorage`] write port the
//! driver persists the registry through, and the default in-memory
//! [`MemMatchmakerStorage`].
//!
//! The shape mirrors [`NodeStorage`](crate::NodeStorage): the core only ever
//! *reads back* its durable state once, at construction
//! ([`MatchmakerStorage::initial_state`]); the driver owns every write and
//! applies each [`MatchmakerWriteOp`](paros_core::MatchmakerWriteOp) through the
//! matching method here, then [`sync`](MatchmakerStorage::sync)s the batch
//! **before** its reply leaves (persist-before-reply). Every write returns
//! [`Result`] so faults are injectable from the start; the records ride the
//! existing durable-record contract (`docs/analysis/storage/clstore-record-contract.md`)
//! — there is no matchmaker-specific disk-fault story.

use paros_core::{AcceptorConfig, Ballot, MatchmakerState};

use crate::storage::StorageError;

/// The write side of matchmaker storage: semantic per-record ops over the
/// registry and the watermark scalar.
pub trait MatchmakerStorage {
    /// Boot-time integrity scan, run once per incarnation **before** the core
    /// reads the store (the [`NodeStorage::boot_scan`](crate::NodeStorage::boot_scan)
    /// twin). The default reports a clean store, for storage that cannot rot.
    ///
    /// # Errors
    /// Returns the first classified [`StorageError`] whose verdict requires a
    /// crash.
    fn boot_scan(&mut self) -> Result<(), StorageError> {
        Ok(())
    }

    /// The durable registry and watermark to boot the matchmaker with. Called
    /// once, at construction; a fresh store returns
    /// [`MatchmakerState::default`].
    fn initial_state(&self) -> MatchmakerState;

    /// Persist the registration of `config` under `ballot`. Append-only: the
    /// core only ever registers strictly above the highest ballot it holds.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the durable write fails.
    fn register(&mut self, ballot: Ballot, config: &AcceptorConfig) -> Result<(), StorageError>;

    /// Persist a raised GC watermark and drop every registration below it.
    /// Monotone: a store never lowers its watermark.
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

/// The library's default in-memory matchmaker storage.
#[derive(Clone, Debug, Default)]
pub struct MemMatchmakerStorage {
    state: MatchmakerState,
}

impl MemMatchmakerStorage {
    /// A fresh, empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl MatchmakerStorage for MemMatchmakerStorage {
    fn initial_state(&self) -> MatchmakerState {
        self.state.clone()
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    fn register(&mut self, ballot: Ballot, config: &AcceptorConfig) -> Result<(), StorageError> {
        self.state.registry.insert(ballot, config.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = watermark.round))]
    fn set_gc_watermark(&mut self, watermark: Ballot) -> Result<(), StorageError> {
        if watermark > self.state.gc_watermark {
            self.state.gc_watermark = watermark;
            self.state.registry = self.state.registry.split_off(&watermark);
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
/// a clean reboot of the same store (every read-back goes through it, because
/// the core reads durable state once, at construction).
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
    use paros_core::{NodeId, QuorumSystem};
    let ballot = |round: u64| Ballot {
        round,
        node: NodeId(1),
    };
    let config = |n: u64| {
        AcceptorConfig::new(
            (0..n).map(NodeId).collect::<Vec<_>>(),
            QuorumSystem::Majority,
        )
    };

    // A fresh store is empty, and registrations round-trip through a sync.
    let s = fresh();
    assert_eq!(
        s.initial_state(),
        MatchmakerState::default(),
        "a fresh store holds an empty registry at the zero watermark"
    );
    let mut s = reopen(s);
    s.register(ballot(1), &config(3)).expect("register 1");
    s.register(ballot(2), &config(4)).expect("register 2");
    s.sync().expect("sync");
    let mut s = reopen(s);
    let state = s.initial_state();
    assert_eq!(
        state.registry.keys().copied().collect::<Vec<_>>(),
        vec![ballot(1), ballot(2)],
        "registrations read back in ballot order"
    );
    assert_eq!(state.registry[&ballot(1)], config(3));
    assert_eq!(state.registry[&ballot(2)], config(4));
    assert_eq!(state.gc_watermark, Ballot::zero());

    // A raised watermark is durable and drops the collected prefix.
    s.register(ballot(3), &config(5)).expect("register 3");
    s.set_gc_watermark(ballot(2)).expect("raise");
    s.sync().expect("sync raise");
    let mut s = reopen(s);
    let state = s.initial_state();
    assert_eq!(state.gc_watermark, ballot(2), "the watermark round-trips");
    assert_eq!(
        state.registry.keys().copied().collect::<Vec<_>>(),
        vec![ballot(2), ballot(3)],
        "registrations below the watermark are dropped"
    );

    // The watermark never lowers.
    s.set_gc_watermark(ballot(1)).expect("re-raise lower");
    s.sync().expect("sync no-op");
    let s = reopen(s);
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
