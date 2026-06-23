//! Node storage — the read-only [`Storage`] recovery port (from `paros-core`)
//! plus the [`NodeStorage`] write extension the driver persists through, and the
//! default in-memory [`MemStorage`] implementing both.

use paros_core::{Ballot, Config, HardState, Slot, Storage, Value};

/// The write side of node storage.
///
/// [`paros_core::Storage`] is the read-only recovery port — the core only ever
/// *reads back* durable state. The driver, which owns all writes, persists
/// through this extension **before** sending the matching batch's messages (the
/// persist-before-send rule [`paros_core::Ready`] documents). Implementors also
/// implement [`Storage`]. Stage 1 only needs `set_hard_state`; later stages add
/// log-append and snapshot writes here.
pub trait NodeStorage: Storage {
    /// Persist the durable [`HardState`].
    fn set_hard_state(&mut self, hard_state: HardState);
}

/// The smallest thing that satisfies [`paros_core::Storage`]: enough to
/// *construct* a [`paros_core::RawNode`] and to receive the durable-before-send
/// writes the driver makes while draining a [`paros_core::Ready`].
///
/// This is the library's default in-memory storage. A crash-testable faulty fake
/// (fail-stop, corruption, protocol-aware recovery) arrives with the
/// storage-fault milestone (Stage 4+); the driver is generic over
/// [`paros_core::Storage`], so it swaps in without touching the loop.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hard_state: HardState,
    config: Config,
}

impl MemStorage {
    /// A fresh, empty storage for a node with the given identity/membership.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            hard_state: HardState::default(),
            config,
        }
    }
}

impl NodeStorage for MemStorage {
    fn set_hard_state(&mut self, hard_state: HardState) {
        self.hard_state = hard_state;
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state.clone(), self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Value)> {
        self.hard_state.accepted.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        Slot(0)
    }

    fn last_slot(&self) -> Slot {
        self.hard_state
            .accepted
            .keys()
            .next_back()
            .copied()
            .unwrap_or(Slot(0))
    }

    fn snapshot(&self) -> Option<Vec<u8>> {
        None
    }
}
