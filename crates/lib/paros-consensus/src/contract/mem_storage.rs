//! [`MemStorage`]: an in-memory [`Storage`] that a driver writes through
//! [`MemStorage::apply`], for tests, examples and the browser game.

use std::collections::BTreeMap;

use crate::contract::config::Config;
use crate::contract::hard_state::HardState;
use crate::contract::storage::Storage;
use crate::contract::write::{AcceptorWrite, RetireMark, WriteOp};
use crate::journal::Command;
use crate::journal::state::JournalState;
use crate::types::{Ballot, Slot};

/// An in-memory store: the durable scalars, the accepted log, the
/// compaction floor and the sealed journal state of one deployment, as a
/// [`Storage`] the core boots from. A driver persists each `Ready`'s writes
/// with [`MemStorage::apply`], in order, before it sends the batch's
/// messages; a restart is a new deployment built from the same store.
/// Nothing here is lost unless the caller rots a record
/// ([`MemStorage::rot`]): the disk faults of a test or a simulation.
///
/// ```
/// use paros_consensus::contract::{AcceptorWrite, MemStorage};
/// use paros_consensus::journal::JournalIdentifier;
/// use paros_consensus::{Ballot, Config, NodeId, WriteOp};
///
/// let config = Config::new(NodeId(1), JournalIdentifier::UNSET).with_peers([NodeId(1)]);
/// let mut disk = MemStorage::new(config);
/// let ballot = Ballot { round: 3, node: NodeId(1) };
/// // A driver passes `ready.writes()` here; this is one write by hand.
/// disk.apply(&[WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot))]);
/// assert_eq!(disk.hard_state().max_promised_ballot, ballot);
/// ```
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
    /// The registration fence (#1002 (retire fence gaps)).
    pub(crate) registration_fence: Ballot,
    /// The retire mark (#1037 (rejoin)).
    pub(crate) retire_mark: RetireMark,
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
            registration_fence: Ballot::zero(),
            retire_mark: RetireMark::default(),
        }
    }

    /// Persists one `Ready`'s writes, in order.
    ///
    /// # Panics
    ///
    /// If a write lowers the promise, the floor or the registration fence:
    /// the core never asks for that.
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
                // A write at a slot replaces a damaged copy there: the
                // record is whole again.
                self.faulty.retain(|(at, _)| at != slot);
                self.accepted.insert(*slot, (*ballot, command.clone()));
            }
            WriteOp::SetChosenIndex(slot) => self.hard_state.chosen_index = Some(*slot),
            WriteOp::Truncate { first, sealed } => {
                self.sealed = sealed.clone();
                self.raise_floor(*first);
            }
            WriteOp::TrimmedTo { point, state } => {
                self.sealed = state.clone();
                let boundary = Slot(point.0 - 1);
                self.hard_state.chosen_index = self.hard_state.chosen_index.max(Some(boundary));
                self.raise_floor(*point);
            }
            WriteOp::RegistrationFence(ballot) => {
                // Write half of the fence pair: the core only ever raises it.
                assert!(
                    *ballot > self.registration_fence,
                    "a persisted registration fence only rises"
                );
                self.registration_fence = *ballot;
            }
            WriteOp::RetireMark(mark) => {
                // Write half of the mark pair: the incarnation never falls.
                assert!(
                    mark.incarnation >= self.retire_mark.incarnation,
                    "a persisted incarnation never falls"
                );
                self.retire_mark = *mark;
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

    /// Rot the accepted record at `slot`: its value is lost and its
    /// identity `(slot, ballot)` survives, the record a boot scan classifies
    /// as recoverable faulty ([`Storage::faulty_entries`]). Returns the
    /// ballot of the rotted record. A later write at `slot` makes it whole.
    ///
    /// # Panics
    ///
    /// If the store holds no readable record at `slot`.
    pub fn rot(&mut self, slot: Slot) -> Ballot {
        let (ballot, _) = self
            .accepted
            .remove(&slot)
            .expect("rot targets a persisted record");
        assert!(
            self.faulty.iter().all(|(at, _)| *at != slot),
            "a readable record is not also faulty"
        );
        self.faulty.push((slot, ballot));
        ballot
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
    fn last_slot(&self) -> Option<Slot> {
        self.accepted.keys().next_back().copied()
    }
    fn sealed_state(&self) -> JournalState {
        self.sealed.clone()
    }
    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty.clone()
    }
    fn registration_fence(&self) -> Ballot {
        self.registration_fence
    }
    fn retire_mark(&self) -> RetireMark {
        self.retire_mark
    }
}

#[cfg(test)]
mod tests;
