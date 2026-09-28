//! The library's default in-memory [`NodeStorage`]: the durable scalars and
//! the per-slot accepted log kept apart, never a single blob.

use std::collections::BTreeMap;

use paros_core::{Ballot, Command, Config, HardState, MustSync, SessionEntry, Slot, Storage};

use super::{NodeStorage, SNAP_CHUNK_BYTES, StorageError, snap_chunk_count};

/// The library's default in-memory storage: enough to *construct* a
/// [`paros_core::ColocatedNode`] and to receive the semantic writes the driver makes
/// while draining a [`paros_core::Ready`]. The durable scalars and the per-slot
/// accepted log are stored separately (never a single blob).
///
/// The crash-testable faulty store (fail-stop, corruption, protocol-aware
/// recovery) is the harness's world-backed disk in `paros-sim`; the driver is
/// generic over [`NodeStorage`], so it swaps in without touching the loop.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard_state: HardState,
    accepted: BTreeMap<Slot, (Ballot, Command)>,
    config: Config,
    /// The compaction floor: the first slot still retained. Everything below it
    /// has been truncated away.
    first: Slot,
    /// Sealed at-most-once ledger records for truncated slots, keyed by
    /// `(client, seq)` (see [`NodeStorage::truncate`]).
    sealed: BTreeMap<(paros_core::ClientId, paros_core::ClientSeq), Slot>,
    /// The latest decided snapshot point (#101): `(marker slot, blob)`.
    snap_point: Option<(Slot, Vec<u8>)>,
    /// The format marker (#147): set by [`NodeStorage::format`], never
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
            sealed: BTreeMap::new(),
            snap_point: None,
            formatted: false,
        }
    }

    /// A storage rebuilt from durable records already read back: the scalars,
    /// the compaction floor, the retained accepted log and the sealed ledger.
    /// Synchronous by design — this is the in-memory index a boot loads, the
    /// one the core's read-only [`Storage`] port is answered from once the
    /// async [`NodeStorage::boot_scan`] has brought the records in. Records
    /// below `first` are dropped, exactly as a durable [`truncate`](NodeStorage::truncate)
    /// would have left them.
    #[must_use]
    pub fn from_records(
        config: Config,
        hard_state: HardState,
        first: Slot,
        accepted: impl IntoIterator<Item = (Slot, Ballot, Command)>,
        sealed: &[SessionEntry],
    ) -> Self {
        let mut storage = Self {
            hard_state,
            accepted: accepted
                .into_iter()
                .filter(|(slot, _, _)| *slot >= first)
                .map(|(slot, ballot, command)| (slot, (ballot, command)))
                .collect(),
            config,
            first,
            sealed: BTreeMap::new(),
            snap_point: None,
            // Records read back from a formatted store: the marker was
            // written before any of them could be.
            formatted: true,
        };
        storage.seal(sealed);
        storage
    }

    fn seal(&mut self, sealed: &[SessionEntry]) {
        for &(client, seq, slot) in sealed {
            self.sealed.entry((client, seq)).or_insert(slot);
        }
    }
}

impl NodeStorage for MemStorage {
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

    #[tracing::instrument(level = "debug", skip_all, fields(first = first.0, sealed = sealed.len()))]
    async fn truncate(&mut self, first: Slot, sealed: &[SessionEntry]) -> Result<(), StorageError> {
        self.seal(sealed);
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn snapshot(&self) -> Vec<u8> {
        // The default in-memory storage has no application state machine, so its
        // opaque "snapshot" is a deterministic marker of the chosen prefix. A real
        // application supplies a NodeStorage whose snapshot() folds its own state.
        self.hard_state
            .chosen_index
            .map_or_else(Vec::new, |ci| ci.0.to_le_bytes().to_vec())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn install_snapshot(
        &mut self,
        chosen_index: Slot,
        ballot: Ballot,
        _snapshot: Vec<u8>,
        sessions: &[SessionEntry],
    ) -> Result<(), StorageError> {
        self.seal(sessions);
        self.hard_state.chosen_index = Some(chosen_index);
        self.hard_state.max_promised_ballot = self.hard_state.max_promised_ballot.max(ballot);
        let first = Slot(chosen_index.0 + 1);
        self.first = self.first.max(first);
        self.accepted.retain(|slot, _| *slot >= self.first);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn apply(
        &mut self,
        _chosen_index: Slot,
        _slot: Slot,
        _command: &Command,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    fn applied_slot(&self) -> Option<Slot> {
        self.hard_state.chosen_index
    }

    #[tracing::instrument(level = "debug", skip_all, fields(at = at.0))]
    async fn record_snapshot(&mut self, at: Slot) -> Result<(), StorageError> {
        self.snap_point = Some((at, self.snapshot().await));
        Ok(())
    }

    fn latest_snap_point(&self) -> Option<Slot> {
        self.snap_point.as_ref().map(|(at, _)| *at)
    }

    fn snap_chunk_count(&self, at: Slot) -> Option<u32> {
        self.snap_point
            .as_ref()
            .filter(|(point, _)| *point == at)
            .map(|(_, blob)| snap_chunk_count(blob.len()))
    }

    #[tracing::instrument(level = "trace", skip_all, fields(at = at.0, chunk))]
    async fn read_snap_chunk(&self, at: Slot, chunk: u32) -> Option<Vec<u8>> {
        let (point, blob) = self.snap_point.as_ref()?;
        if *point != at {
            return None;
        }
        let start = usize::try_from(chunk).ok()?.checked_mul(SNAP_CHUNK_BYTES)?;
        if start >= blob.len() {
            return None;
        }
        let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
        Some(blob[start..end].to_vec())
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_snap_chunk(
        &mut self,
        at: Slot,
        chunk: u32,
        bytes: &[u8],
    ) -> Result<bool, StorageError> {
        let Some((point, blob)) = self.snap_point.as_mut() else {
            return Ok(false);
        };
        if *point != at {
            return Ok(false);
        }
        let Some(start) = usize::try_from(chunk)
            .ok()
            .and_then(|c| c.checked_mul(SNAP_CHUNK_BYTES))
        else {
            return Ok(false);
        };
        if start >= blob.len() {
            return Ok(false);
        }
        let end = (start + SNAP_CHUNK_BYTES).min(blob.len());
        if bytes.len() != end - start {
            return Ok(false);
        }
        blob[start..end].copy_from_slice(bytes);
        // In-memory chunks cannot rot; a write leaves the point fully clean.
        Ok(true)
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

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect()
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
