//! The library's default in-memory [`LogStorage`]: the durable scalars and
//! the per-slot accepted log kept apart, never a single blob.

use std::collections::BTreeMap;

use paros_core::{Ballot, Command, Config, HardState, JournalState, MustSync, Slot, Storage};

use super::{LogStorage, StorageError};

/// The library's default in-memory storage: enough to *construct* a
/// [`paros_core::ColocatedNode`] and to receive the semantic writes the driver makes
/// while draining a [`paros_core::Ready`]. The durable scalars and the per-slot
/// accepted log are stored separately (never a single blob).
///
/// The crash-testable faulty store (fail-stop, corruption, protocol-aware
/// recovery) is the harness's world-backed disk in `paros-sim`; the driver is
/// generic over [`LogStorage`], so it swaps in without touching the loop.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    config: Config,
    /// The compaction floor: the first slot still retained. Everything below it
    /// has been truncated away.
    first: Slot,
    /// The journal state sealed at the floor (see [`LogStorage::truncate`]).
    sealed: JournalState,
    /// The format marker (#147): set by [`LogStorage::format`], never
    /// cleared.
    formatted: bool,
}

impl MemStorage {
    /// A fresh, empty storage for a node with the given identity/membership.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            hard_state: HardState::default(),
            accepted: BTreeMap::new(),
            config,
            first: Slot(0),
            sealed: JournalState::default(),
            formatted: false,
        }
    }

    /// A storage rebuilt from durable records already read back: the scalars,
    /// the compaction floor, the retained accepted log and the sealed state.
    /// Synchronous by design — this is the in-memory index a boot loads, the
    /// one the core's read-only [`Storage`] port is answered from once the
    /// async [`LogStorage::boot_scan`] has brought the records in. Records
    /// below `first` are dropped, exactly as a durable [`truncate`](LogStorage::truncate)
    /// would have left them.
    #[must_use]
    pub fn from_records(
        config: Config,
        hard_state: HardState,
        first: Slot,
        accepted: impl IntoIterator<Item = (Slot, Ballot, Command)>,
        sealed: JournalState,
    ) -> Self {
        Self {
            hard_state,
            accepted: accepted
                .into_iter()
                .filter(|(slot, _, _)| *slot >= first)
                .map(|(slot, ballot, command)| (slot, (ballot, command)))
                .collect(),
            config,
            first,
            sealed,
            // Records read back from a formatted store: the marker was
            // written before any of them could be.
            formatted: true,
        }
    }

    /// Raise the floor to `first`, sealing `state` with it; a floor that
    /// does not rise keeps the state sealed with the higher one.
    fn raise_floor(&mut self, first: Slot, state: JournalState) {
        if first >= self.first {
            self.sealed = state;
        }
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
    }
}

impl LogStorage for MemStorage {
    fn is_formatted(&self) -> bool {
        self.formatted
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn format(&mut self) -> Result<(), StorageError> {
        self.formatted = true;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(round = ballot.round))]
    async fn persist_ballot(&mut self, ballot: Ballot) -> Result<(), StorageError> {
        self.hard_state.max_promised_ballot = ballot;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn append_accepted(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: Command,
    ) -> Result<(), StorageError> {
        self.accepted.insert(slot, (ballot, command));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all, fields(slot = slot.0))]
    async fn set_chosen_index(&mut self, slot: Slot) -> Result<(), StorageError> {
        self.hard_state.chosen_index = Some(slot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn sync(&mut self, _must_sync: MustSync) -> Result<(), StorageError> {
        // In-memory: writes are already visible; nothing to flush.
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(first = first.0))]
    async fn truncate(&mut self, first: Slot, sealed: JournalState) -> Result<(), StorageError> {
        self.raise_floor(first, sealed);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(point = point.0))]
    async fn trimmed_to(&mut self, point: Slot, state: JournalState) -> Result<(), StorageError> {
        let boundary = Slot(point.0.saturating_sub(1));
        if self.hard_state.chosen_index.is_none_or(|ci| ci < boundary) {
            self.hard_state.chosen_index = Some(boundary);
        }
        self.raise_floor(point, state);
        Ok(())
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state, self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.first
    }

    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }

    fn sealed_state(&self) -> JournalState {
        self.sealed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_contract_suite;
    use paros_core::NodeId;

    /// The shared behavioral contract, against the library's default storage.
    /// The simulation runs the same suite against its world-backed storage.
    #[test]
    fn mem_storage_passes_the_contract_suite() {
        futures::executor::block_on(storage_contract_suite(
            || {
                std::future::ready(MemStorage::new(Config {
                    id: NodeId(0),
                    peers: vec![NodeId(0)],
                    ..Config::default()
                }))
            },
            // In-memory writes are immediately visible: a reboot is the same
            // handle.
            std::future::ready,
        ));
    }
}
