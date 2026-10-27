//! [`MemStorage`]: an in-memory [`Storage`] that a driver writes through
//! [`MemStorage::apply`], for tests, examples and the browser game.

use std::collections::BTreeMap;

use crate::contract::config::Config;
use crate::contract::hard_state::HardState;
use crate::contract::storage::Storage;
use crate::contract::write::{AcceptorWrite, WriteOp};
use crate::journal::Command;
use crate::journal::state::JournalState;
use crate::types::{Ballot, Slot};

/// An in-memory store: the durable scalars, the accepted log, the
/// compaction floor and the sealed journal state of one deployment, as a
/// [`Storage`] the core boots from. A driver persists each `Ready`'s writes
/// with [`MemStorage::apply`], in order, before it sends the batch's
/// messages; a restart is a new deployment built from the same store.
/// Nothing here is ever lost, so it models a disk that never fails.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemStorage {
    pub(crate) config: Config,
    pub(crate) hard_state: HardState,
    pub(crate) accepted: BTreeMap<Slot, (Ballot, Command)>,
    pub(crate) first_slot: Slot,
    /// Recoverable faulty entries a boot scan would classify (Stage 8).
    pub(crate) faulty: Vec<(Slot, Ballot)>,
    /// The journal state sealed at the floor (#204).
    pub(crate) sealed: JournalState,
}

impl MemStorage {
    /// A fresh store for a deployment under `config`: nothing promised,
    /// nothing accepted, nothing chosen.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            hard_state: HardState::default(),
            accepted: BTreeMap::new(),
            first_slot: Slot(0),
            faulty: Vec::new(),
            sealed: JournalState::default(),
        }
    }

    /// Persists one `Ready`'s writes, in order.
    ///
    /// # Panics
    ///
    /// If a write lowers the promise or the floor: the core never asks
    /// for that.
    pub fn apply(&mut self, writes: &[WriteOp]) {
        for write in writes {
            self.apply_one(write);
        }
    }

    fn apply_one(&mut self, write: &WriteOp) {
        match write {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => {
                assert!(
                    *ballot >= self.hard_state.max_promised_ballot,
                    "a persisted promise never falls"
                );
                self.hard_state.max_promised_ballot = *ballot;
            }
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            })
            | WriteOp::Learned {
                slot,
                ballot,
                command,
            } => {
                self.accepted.insert(*slot, (*ballot, command.clone()));
            }
            WriteOp::SetChosenIndex(slot) => self.hard_state.chosen_index = Some(*slot),
            WriteOp::Truncate { first, sealed } => {
                self.sealed = *sealed;
                self.raise_floor(*first);
            }
            WriteOp::TrimmedTo { point, state } => {
                self.sealed = *state;
                let boundary = Slot(point.0 - 1);
                if self.hard_state.chosen_index.is_none_or(|ci| ci < boundary) {
                    self.hard_state.chosen_index = Some(boundary);
                }
                self.raise_floor(*point);
            }
        }
    }

    fn raise_floor(&mut self, first: Slot) {
        let floor = self.first_slot.max(first);
        self.accepted = self.accepted.split_off(&floor);
        self.faulty.retain(|(slot, _)| *slot >= floor);
        self.first_slot = floor;
        assert!(
            self.accepted
                .keys()
                .next()
                .is_none_or(|slot| *slot >= floor),
            "a raised floor drops every record below it"
        );
    }

    /// The configuration this store boots its deployment under.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The durable scalars persisted so far.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        self.hard_state
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
        self.first_slot
    }
    fn last_slot(&self) -> Slot {
        self.accepted.keys().next_back().copied().unwrap_or(Slot(0))
    }
    fn sealed_state(&self) -> JournalState {
        self.sealed
    }
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty.clone()
    }
}
