//! A minimal in-memory [`Storage`] for the Stage-1 driver.

use paros_core::{Ballot, Config, HardState, Slot, Storage, Value};

/// The smallest thing that satisfies [`paros_core::Storage`]: enough to
/// *construct* a [`paros_core::RawNode`] and to receive the durable-before-send
/// writes the driver makes while draining a [`paros_core::Ready`].
///
/// Stage 1 has no protocol, so this never holds real Paxos state — the faulty,
/// crash-testable storage fake is Stage 4+ (`paros-storage`). The point here is
/// only that the *write path* the driver exercises has somewhere to land.
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

    /// Persist `hard_state`. The driver calls this *before* sending the matching
    /// batch's messages — the persist-before-send ordering [`paros_core::Ready`]
    /// documents.
    pub fn set_hard_state(&mut self, hard_state: HardState) {
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
